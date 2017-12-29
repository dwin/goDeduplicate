package dedup

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/minio/blake2b-simd"
)

type File struct {
	Pathname      string
	Blake2bHEX    string
	DupOfPosition int
	Error         error
}

func DeduplicateToNew(orig, dest string) (err error) {
	results, err := FindDuplicates(orig)
	if err != nil {
		return err
	}
	return filesToFolderExcludeDup(orig, dest, results)
}

func DeduplicateByDeletion(orig string) (err error) {
	results, err := FindDuplicates(orig)
	if err != nil {
		return err
	}
	return removeDuplicates(results)
}

func removeDuplicates(files []File) (err error) {
	for i := 0; i < len(files); i++ {
		// Remove file if duplicate
		if files[i].DupOfPosition != 0 {
			err := os.Remove(files[i].Pathname)
			if err != nil {
				return fmt.Errorf("removeDuplicates os.Remove err: %s", err)
			}
		}
	}
	return
}

// filesToFolderExcludeDup will create copies of the original files excluding the duplicates, a directory will be created if it does not exist.
func filesToFolderExcludeDup(original, destination string, files []File) error {
	// Create new folder for files in destination directory
	err := os.MkdirAll(destination, 0755)
	if err != nil {
		return fmt.Errorf("filesToFolderExcludeDup error: %s", err)
	}
	// Loop through files and save to new directory with same filename
	for i := 0; i < len(files); i++ {
		if files[i].DupOfPosition != 0 {
			continue
		}
		var newFn string // new filename, same unless exists in destination
		if _, err := os.Stat(destination + "/" + strings.TrimPrefix(files[i].Pathname, original)); os.IsExist(err) {
			newFn = fmt.Sprintf("%s/(%v)-%s", destination, strings.TrimPrefix(files[i].Pathname, original), i)
		} else {
			newFn = fmt.Sprintf("%s/%s", destination, strings.TrimPrefix(files[i].Pathname, original))
		}
		orig, err := os.Open(files[i].Pathname)
		if err != nil {
			return fmt.Errorf("filesToFolderExcludeDup open orig error: %s", err)
		}
		defer orig.Close()
		newF, err := os.OpenFile(newFn, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			return fmt.Errorf("filesToFolderExcludeDup open new error: %s", err)
		}
		defer newF.Close()
		_, err = io.Copy(newF, orig)
		if err != nil {
			return fmt.Errorf("filesToFolderExcludeDup io.Copy error: %s", err)
		}

	}
	return err
}

func FindDuplicates(dir string) (files []File, err error) {
	// Number of threads to use
	maxThreads := 4

	filenames, err := getDirFiles(dir)
	if err != nil {
		return
	}
	//fmt.Println("Filenames: ", len(filenames))
	// Create appropriate sized channel for hashing
	jobs := make(chan File, len(filenames))
	results := make(chan File, len(filenames))

	// Start downloaders
	for thread := 0; thread < maxThreads; thread++ {
		//fmt.Println("Starting thread: ", thread)
		go hashFile(thread, jobs, results)
	}
	var jobcount int
	// Loop through all filenames returned from getDirFiles and send to jobs queue
	for i := 0; i < len(filenames); i++ {
		// Create file and send to fileHash
		//fmt.Println("Sending file: ", i)
		jobs <- File{Pathname: dir + "/" + filenames[i]}
		jobcount++
	}
	close(jobs)
	for i := 0; i < len(filenames)-1; i++ {
		f := <-results
		//fmt.Println("received result ", i)
		files = append(files, f)
	}
	checked := checkDuplicate(files)

	return checked, err
}

func checkDuplicate(files []File) []File {
	for a := 0; a < len(files)-1; a++ {
		for b := a + 1; b < len(files); b++ {
			//fmt.Printf("Comparing files[a](%v) to files[b](%v)\n", a, b)
			if files[a].Blake2bHEX == files[b].Blake2bHEX {
				files[b].DupOfPosition = a
			}
		}
	}
	return files
}

func getDirFiles(directory string) (result []string, err error) {
	dir, err := os.Open(directory)
	defer dir.Close()
	if err != nil {
		return result, fmt.Errorf("getDirFiles open file error: %s", err)
	}
	info, err := dir.Stat()
	if err != nil {
		return result, fmt.Errorf("getDirFiles file stat error: %s", err)
	}
	// Verify directory string points to directory
	if !info.IsDir() {
		// If directory given is not directory, return err
		return result, fmt.Errorf("getDirFiles error: path given is not a directory")
	}
	result, err = dir.Readdirnames(-1)
	if err != nil {
		return result, fmt.Errorf("getDirFiles Read Directory names error: %s", err)
	}
	return
}

func hashFile(worker int, jobs <-chan File, results chan<- File) {
	for j := range jobs {
		result := File{Pathname: j.Pathname}
		//fmt.Println("Starting: ", j.Pathname)
		hash := blake2b.New512()
		file, err := os.Open(j.Pathname)
		if err != nil {
			fmt.Println("Cannot open file, err: ", err)
			result.Error = fmt.Errorf("hashFile open file error: %s", err)
			continue
		}
		defer file.Close()
		_, err = io.Copy(hash, file)
		if err != nil {
			fmt.Println("Cannot hash file")
			result.Error = fmt.Errorf("hashFile io.Copy error: %s", err)
			continue
		}
		result.Blake2bHEX = hex.EncodeToString(hash.Sum(nil))
		//fmt.Printf("Result for %s Hex: %s", result.Pathname, result.Blake2bHEX)
		results <- result
	}

}
