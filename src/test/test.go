package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

type stu struct {
	name string
	id   int
}

func main() {
	name := "wang"
	id := 1
	w := bytes.NewBuffer(nil)
	e := gob.NewEncoder(w)
	e.Encode(name)
	e.Encode(id)
	data := w.Bytes()
	fmt.Println("len data", len(data))

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	name = ""
	id = -1
	d.Decode(&name)
	d.Decode(&id)

	fmt.Println("result", name, id)

}
