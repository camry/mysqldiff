package main

import (
    "log"

    "mysqldiff/cmd"
)

func main() {
    if err := cmd.Execute(); err != nil {
        log.Fatalln(err)
    }
}
