package update

import "github.com/azhai/goent/model"

// Set is used inside a update to set a value on the column for all the matched records
//
// # Example
//
//	// set animal name as cat on record of id 2
//	err = goent.Update(db.Animal).Sets(update.Sets(&db.Animal.Name, "Cat")).
//	Where(where.Equals(&db.Animal.Id, 2))
func Set[T any, A *T | **T](a A, v T) model.Set {
	return model.Set{Attribute: a, Value: v}
}
