package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func IsDirEmpty(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdirnames(1) // Or f.Readdir(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err // Either not empty or error, suits both cases
}

func hashFileMd5(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	hashInBytes := hash.Sum(nil)[:16]
	return hex.EncodeToString(hashInBytes), nil
}

func checkError(e error) {
	if e != nil {
		fmt.Println(e)
		os.Exit(1)
	}
}

func readlines(path string) ([]string, error) {
	file, err := os.Open(path)
	checkError(err)
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

type quick struct {
	md5       string
	timestamp string
	size      int64
	path      string
}

type empty struct {
	path string
}

type link struct {
	src string
	dst string
}

func main() {
	if len(os.Args) == 3 {
		root := os.Args[1]
		currentQuicksMd5, err := getCurrentQuicksMd5(root)
		switch os.Args[2] {
		case "generate":
			os.Exit(generate(root, currentQuicksMd5))
		case "check":
			checkError(err)
			os.Exit(check(root, currentQuicksMd5))
		}
	}
	os.Exit(usage())
}

func getCurrentQuicksMd5(root string) (currentQuicksMd5 []quick, err error) {
	quickTxtPath := root + "/quick.txt"
	if _, err = os.Stat(quickTxtPath); err == nil { // quick.txt существует.
		var lines []string
		lines, err = readlines(quickTxtPath)
		for _, line := range lines {
			if strings.HasPrefix(line, "md5 ") {
				words := strings.SplitN(line, " ", 5)
				md5 := words[1]
				timestamp := words[2]
				size, _ := strconv.ParseInt(words[3], 10, 64)
				path := words[4]
				currentQuicksMd5 = append(currentQuicksMd5,
					quick{md5: md5, timestamp: timestamp, size: size, path: path})
			}
		}
	}
	return currentQuicksMd5, err
}

func generate(root string, currentQuicksMd5 []quick) int {
	var emptys []empty
	var updatedQuicks []quick
	var links []link
	err := filepath.Walk(root,
		func(path string, info os.FileInfo, err error) error {
			checkError(err)
			rel, err := filepath.Rel(root, path)
			if rel == "quick.txt" {
				return nil
			}
			checkError(err)
			if info.Mode().IsRegular() {
				timestamp := info.ModTime().Format("20060102150405")
				size := info.Size()
				md5 := ""
				for i := range currentQuicksMd5 {
					if currentQuicksMd5[i].path == rel { // Нашли проверяемый файл в текущем quick.txt.
						if currentQuicksMd5[i].timestamp == timestamp && currentQuicksMd5[i].size == size {
							md5 = currentQuicksMd5[i].md5
						}
						break
					}
				}
				if md5 == "" {
					md5, err = hashFileMd5(path)
					checkError(err)
				}
				updatedQuicks = append(updatedQuicks, quick{md5: md5, timestamp: timestamp, size: size, path: rel})
			} else if info.Mode()&os.ModeSymlink != 0 {
				src, err := os.Readlink(path)
				checkError(err)
				links = append(links, link{src: src, dst: rel})
			} else if info.IsDir() {
				isEmpty, err := IsDirEmpty(path)
				checkError(err)
				if isEmpty {
					emptys = append(emptys, empty{rel})
				}
			}
			return nil
		})
	checkError(err)

	f, err := os.Create(root + "/quick.txt_")
	checkError(err)
	defer f.Close()
	for i := range emptys {
		_, err := f.WriteString(fmt.Sprintf("empty %s\n", emptys[i].path))
		checkError(err)
	}
	for i := range links {
		_, err := f.WriteString(fmt.Sprintf("link %s>%s\n", links[i].dst, links[i].src))
		checkError(err)
	}
	for i := range updatedQuicks {
		_, err := f.WriteString(fmt.Sprintf("md5 %s %s %d %s\n", updatedQuicks[i].md5, updatedQuicks[i].timestamp, updatedQuicks[i].size, updatedQuicks[i].path))
		checkError(err)
	}
	f.Sync()
	checkError(err)
	err = os.Rename(root+"/quick.txt_", root+"/quick.txt")
	checkError(err)
	return 0
}

func check(root string, currentQuicksMd5 []quick) int {
	// Ищем check.txt в той же папке, что и quick.txt и читаем из него то, что надо игнорировать:
	var ignorespatterns []string
	checkTxtPath := root + "/check.txt"
	if _, err := os.Stat(checkTxtPath); err == nil {
		var lines []string
		lines, err = readlines(checkTxtPath)
		for _, line := range lines {
			if strings.HasPrefix(line, "ignore ") {
				chunks := strings.SplitN(line, " ", 2)
				ignorespatterns = append(ignorespatterns, chunks[1])
			}
		}
	}

	errors := 0
	for i := range currentQuicksMd5 {
		quickTimestamp := currentQuicksMd5[i].timestamp
		quickSize := currentQuicksMd5[i].size
		rel := currentQuicksMd5[i].path

		// Каждый файл из quick сперва проверяем попадает ли он в список игнорируемых:
		skip := false
		for j := range ignorespatterns {
			pattern := ignorespatterns[j]
			skip, _ = filepath.Match(pattern, rel)
			if skip {
				break
			}
		}
		if skip {
			continue
		}

		path := fmt.Sprintf("%s/%s", root, rel)
		f, err := os.Stat(path)
		if os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "no %s\n", rel)
			errors++
			continue
		}
		realTimestamp := f.ModTime().Format("20060102150405")
		realSize := f.Size()
		if quickSize != realSize && quickTimestamp != realTimestamp {
			quickMd5 := currentQuicksMd5[i].md5
			realMd5, err := hashFileMd5(path)
			if err != nil || quickMd5 != realMd5 {
				errors++
				fmt.Fprintf(os.Stderr, "md5 %s\n", rel)
			}
		}
	}
	if errors > 0 {
		fmt.Fprintln(os.Stderr, errors)
		return 1
	}
	return 0
}

func usage() int {
	fmt.Println("Usage:")
	fmt.Println("  quick [PATH] generate            # Generate PATH/quick.txt")
	fmt.Println("  quick [PATH] check [IGNORE FILE] # Check files using PATH/quick.txt")
	return 1
}
