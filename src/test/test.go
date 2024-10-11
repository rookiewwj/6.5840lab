package main

import "fmt"

func main() {
	arr := []int{1, 2, 3}
	tmp := arr[len(arr):]
	fmt.Println(tmp)

}
