package server

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
)

type File struct {
	mu        sync.RWMutex
	filename  string
	node_size int
}

// 先检查该磁盘是否存在该文件，如不存在则需要创建
func NewFile(path_name string) (file *File, fd *os.File, Err string, err error) {
	if !CheckFileOrList(path_name) {
		fd, err = CreateFile(path_name)
		if err != nil {
			Err = "CreatFileFail"
			DEBUG(dError, err.Error())
			return nil, nil, Err, err
		}
	} else {
		fd, err = os.OpenFile(path_name, os.O_APPEND|os.O_RDWR, os.ModeAppend|os.ModePerm)
		if err != nil {
			DEBUG(dError, err.Error())
			Err = "OpenFile"
			return nil, nil, Err, err
		}
	}

	file = &File{
		mu:        sync.RWMutex{},
		filename:  path_name,
		node_size: NODE_SIZE,
	}
	return file, fd, "ok", err
}

func CheckFile(path_name string) (file *File, fd *os.File, Err string, err error) {
	if !CheckFileOrList(path_name) {

		Err = "NotFile"
		err = errors.New(Err)
		DEBUG(dError, err.Error())
		return nil, nil, Err, err
	} else {
		fd, err = os.OpenFile(path_name, os.O_APPEND|os.O_RDWR, os.ModeAppend|os.ModePerm)
		if err != nil {
			DEBUG(dError, err.Error())
			Err = "OpenFile"
			return nil, nil, Err, err
		}
	}

	file = &File{
		mu:        sync.RWMutex{},
		filename:  path_name,
		node_size: NODE_SIZE,
	}
	return file, fd, "ok", err
}

// 修改文件名
func (f *File) Update(path, file_name string) error {
	OldFilePath := f.filename
	NewFilePath := path + "/" + file_name
	f.mu.Lock()
	f.filename = NewFilePath
	f.mu.Unlock()

	return MovName(OldFilePath, NewFilePath)
}

func (f *File) OpenFile() *os.File {
	f.mu.RLock()
	file, _ := CreateFile(f.filename)
	f.mu.RUnlock()

	return file
}

func (f *File) GetFirstIndex(file *os.File) int64 {
	var node Key
	data_node := make([]byte, NODE_SIZE)

	f.mu.RLock()
	defer f.mu.RUnlock()

	_, err := file.ReadAt(data_node, 0)

	if err == io.EOF {
		//读到文件末尾
		DEBUG(dLeader, "read All file, do not find this index")
		return 0
	}

	json.Unmarshal(data_node, &node)

	return node.Start_index
}

func (f *File) GetIndex(file *os.File) int64 {
	f.mu.RLock()

	f.mu.RUnlock()
	return 0
}

func (f *File) WriteFile(file *os.File, node Key, data_msg []byte) bool {
	data_node := &bytes.Buffer{}
	err := binary.Write(data_node, binary.BigEndian, node)
	if err != nil {
		DEBUG(dError, err.Error())
		DEBUG(dError, "%v turn bytes fail\n", node)
		return false
	}

	f.mu.Lock()

	if f.node_size == 0 {
		f.node_size = len(data_node.Bytes())
	}
	file.Write(data_node.Bytes())
	file.Write(data_msg)

	f.mu.Unlock()

	if err != nil {
		return false
	} else {
		return true
	}
}

func (f *File) ReadFile(file *os.File, offset int64) (Key, []Message, error) {
	var node Key
	var msg []Message
	data_node := make([]byte, NODE_SIZE)

	f.mu.RLock()
	defer f.mu.RUnlock()

	size, err := file.ReadAt(data_node, offset)

	if size != NODE_SIZE {
		return node, msg, errors.New("Read node size is not NODE_SIZE")
	}
	if err == io.EOF {
		return node, msg, errors.New("Read All file, do not find this index")
	}

	buf := &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, data_node)
	binary.Read(buf, binary.BigEndian, &node)
	data_msg := make([]byte, node.Size)
	size, err = file.ReadAt(data_msg, offset+int64(f.node_size)+int64(node.Size))
	offset += int64(f.node_size)
	size, err = file.ReadAt(data_msg, offset)

	// DEBUG(dLog, "the size is %v, data is %v offset is %v, node.Size is %v\n", size, data_node, offset, node.Size)
	if size != node.Size {
		return node, msg, errors.New("read msg size is not NODE_SIZE")
	}
	if err == io.EOF { //读到文件末尾
		return node, msg, errors.New("Read All file, do not find this index")
	}

	json.Unmarshal(data_msg, &msg)

	return node, msg, nil
}

func (f *File) FindOffset(file *os.File, index int64) (int64, error) {
	var node Key
	data_node := make([]byte, NODE_SIZE)

	offset := int64(0)
	for {

		f.mu.RLock()
		size, err := file.ReadAt(data_node, offset)
		f.mu.RUnlock()

		if size != NODE_SIZE {
			return int64(-1), errors.New("Read node size is not NODE_SIZE")
		}
		if err == io.EOF { //读到文件末尾
			return index, errors.New("Read All file, do not find this index")
		}

		buf := &bytes.Buffer{}
		binary.Write(buf, binary.BigEndian, data_node)
		binary.Read(buf, binary.BigEndian, &node)
		if node.End_index < index {
			offset += int64(NODE_SIZE + node.Size)
		} else {
			break
		}
	}

	return offset, nil
}

func (f *File) ReadBytes(file *os.File, offset int64) (Key, []byte, error) {
	var node Key

	data_node := make([]byte, NODE_SIZE)

	f.mu.RLock()
	defer f.mu.RUnlock()

	size, err := file.ReadAt(data_node, offset)

	if size != NODE_SIZE {
		return node, nil, errors.New("read node size is not NODE_SIZE")
	}
	if err == io.EOF { //读到文件末尾
		DEBUG(dLeader, "read All file, do not find this index")
		return node, nil, err
	}

	buf := &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, data_node)
	binary.Read(buf, binary.BigEndian, &node)
	data_msg := make([]byte, node.Size)
	offset += +int64(f.node_size)
	size, err = file.ReadAt(data_msg, offset)

	if size != node.Size {
		return node, nil, errors.New("read msg size is not NODE_SIZE")
	}
	if err == io.EOF { //读到文件末尾
		return node, nil, errors.New("read All file, do not find this index")
	}

	return node, data_msg, nil
}
