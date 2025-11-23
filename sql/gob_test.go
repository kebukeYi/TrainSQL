package sql

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"testing"
)

type Product struct {
	ID        int
	Name      string
	Price     float64
	Category  string
	ExtraData []extraData
}

type extraData struct {
	Name string
	Age  int
}

func gobExample() {
	// 初始化数据
	product := Product{
		ID:       1001,
		Name:     "笔记本电脑",
		Price:    5999.99,
		Category: "电子产品",
		ExtraData: []extraData{
			{Name: "张三", Age: 18},
			{Name: "李四", Age: 20},
		},
	}

	// 序列化
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(product)
	if err != nil {
		log.Fatal("Gob 编码失败:", err)
	}

	fmt.Printf("Gob 二进制数据长度: %d bytes\n", buffer.Len())

	// 反序列化
	var decodedProduct Product
	decoder := gob.NewDecoder(&buffer)
	err = decoder.Decode(&decodedProduct)
	if err != nil {
		log.Fatal("Gob 解码失败:", err)
	}

	fmt.Printf("反序列化结果: %+v\n", decodedProduct)
}

func TestGOB(t *testing.T) {
	gobExample()
}
