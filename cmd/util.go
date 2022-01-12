package cmd

import "reflect"

// InArray will search element inside array with any type.
// Will return boolean and index for matched element.
// True and index more than 0 if element is exist.
// needle is element to search, haystack is slice of value to be search.
func InArray(needle interface{}, haystack interface{}) (exists bool, index int) {
    exists = false
    index = -1

    switch reflect.TypeOf(haystack).Kind() {
    case reflect.Slice:
        s := reflect.ValueOf(haystack)

        for i := 0; i < s.Len(); i++ {
            if reflect.DeepEqual(needle, s.Index(i).Interface()) == true {
                index = i
                exists = true
                return
            }
        }
    }

    return
}
