package goent_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/azhai/goent"
)

// TestO2MForeignQueryReturnsAllChildren verifies that O2M relationship queries
// return ALL child records, not just one. This was a bug where queryOne2ManyReflect
// used foreign.Reference (parent PK column name) instead of foreign.ForeignKey
// (child FK column name) to build the WHERE clause and group results, causing
// only one child to be returned per parent.
func TestO2MForeignQueryReturnsAllChildren(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Clean up
	err = db.UserRole.Delete().Exec()
	if err != nil {
		t.Fatalf("Delete user roles error: %v", err)
	}
	err = db.User.Delete().Exec()
	if err != nil {
		t.Fatalf("Delete users error: %v", err)
	}
	err = db.Role.Delete().Exec()
	if err != nil {
		t.Fatalf("Delete roles error: %v", err)
	}

	// Insert one user
	user := &User{Name: "Alice", Email: "alice10001@test.com", Id: 10001}
	err = db.User.Insert().One(user)
	if err != nil {
		t.Fatalf("Insert user error: %v", err)
	}

	// Insert roles
	role1 := &Role{Name: "Admin", Id: 10001}
	err = db.Role.Insert().One(role1)
	if err != nil {
		t.Fatalf("Insert role error: %v", err)
	}
	role2 := &Role{Name: "Editor", Id: 10002}
	err = db.Role.Insert().One(role2)
	if err != nil {
		t.Fatalf("Insert role error: %v", err)
	}
	role3 := &Role{Name: "Viewer", Id: 10003}
	err = db.Role.Insert().One(role3)
	if err != nil {
		t.Fatalf("Insert role error: %v", err)
	}
	role4 := &Role{Name: "Moderator", Id: 10004}
	err = db.Role.Insert().One(role4)
	if err != nil {
		t.Fatalf("Insert role error: %v", err)
	}

	// Assign all 4 roles to the user
	for i, roleId := range []int{10001, 10002, 10003, 10004} {
		ur := &UserRole{UserId: 10001, RoleId: roleId, Id: 10001 + i}
		err = db.UserRole.Insert().One(ur)
		if err != nil {
			t.Fatalf("Insert user role error: %v", err)
		}
	}

	// Query user with UserRoles (O2M)
	users, err := db.User.Select().With("UserRoles").All()
	if err != nil {
		t.Fatalf("Select users with UserRoles error: %v", err)
	}

	var found *User
	for _, u := range users {
		if u.Name == "Alice" {
			found = u
			break
		}
	}
	if found == nil {
		t.Fatal("Alice not found")
	}

	// BUG: Before fix, only 1 UserRole was returned instead of 4
	if len(found.UserRoles) != 4 {
		t.Errorf("Expected 4 UserRoles for Alice, got %d (bug: O2M returns only 1 child)",
			len(found.UserRoles))
	}
}

// TestO2MForeignQueryMultipleParents verifies O2M works correctly when
// multiple parent records each have their own set of children.
func TestO2MForeignQueryMultipleParents(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Clean up
	err = db.UserRole.Delete().Exec()
	if err != nil {
		t.Fatalf("Delete user roles error: %v", err)
	}
	err = db.User.Delete().Exec()
	if err != nil {
		t.Fatalf("Delete users error: %v", err)
	}
	err = db.Role.Delete().Exec()
	if err != nil {
		t.Fatalf("Delete roles error: %v", err)
	}

	// Insert two users
	alice := &User{Name: "Alice", Email: "alice10101@test.com", Id: 10101}
	err = db.User.Insert().One(alice)
	if err != nil {
		t.Fatalf("Insert user error: %v", err)
	}
	bob := &User{Name: "Bob", Email: "bob10102@test.com", Id: 10102}
	err = db.User.Insert().One(bob)
	if err != nil {
		t.Fatalf("Insert user error: %v", err)
	}

	// Insert roles
	admin := &Role{Name: "Admin", Id: 10101}
	err = db.Role.Insert().One(admin)
	if err != nil {
		t.Fatalf("Insert role error: %v", err)
	}
	editor := &Role{Name: "Editor", Id: 10102}
	err = db.Role.Insert().One(editor)
	if err != nil {
		t.Fatalf("Insert role error: %v", err)
	}
	viewer := &Role{Name: "Viewer", Id: 10103}
	err = db.Role.Insert().One(viewer)
	if err != nil {
		t.Fatalf("Insert role error: %v", err)
	}
	moderator := &Role{Name: "Moderator", Id: 10104}
	err = db.Role.Insert().One(moderator)
	if err != nil {
		t.Fatalf("Insert role error: %v", err)
	}

	// Alice has 3 roles
	for i, roleId := range []int{10101, 10102, 10103} {
		ur := &UserRole{UserId: 10101, RoleId: roleId, Id: 10101 + i}
		err = db.UserRole.Insert().One(ur)
		if err != nil {
			t.Fatalf("Insert user role error: %v", err)
		}
	}

	// Bob has 2 roles
	for i, roleId := range []int{10103, 10104} {
		ur := &UserRole{UserId: 10102, RoleId: roleId, Id: 10104 + i}
		err = db.UserRole.Insert().One(ur)
		if err != nil {
			t.Fatalf("Insert user role error: %v", err)
		}
	}

	// Query all users with UserRoles
	users, err := db.User.Select().With("UserRoles").All()
	if err != nil {
		t.Fatalf("Select users with UserRoles error: %v", err)
	}

	for _, u := range users {
		switch u.Name {
		case "Alice":
			if len(u.UserRoles) != 3 {
				t.Errorf("Alice: expected 3 UserRoles, got %d", len(u.UserRoles))
			}
		case "Bob":
			if len(u.UserRoles) != 2 {
				t.Errorf("Bob: expected 2 UserRoles, got %d", len(u.UserRoles))
			}
		}
	}
}

// TestOneWithForeignLoading verifies that One() correctly loads With() relations.
// Before the fix, One() did not call QueryForeignsByNameContext, so With() was
// silently ignored when fetching a single record.
func TestOneWithForeignLoading(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Clean up
	err = db.UserRole.Delete().Exec()
	if err != nil {
		t.Fatalf("Delete user roles error: %v", err)
	}
	err = db.User.Delete().Exec()
	if err != nil {
		t.Fatalf("Delete users error: %v", err)
	}
	err = db.Role.Delete().Exec()
	if err != nil {
		t.Fatalf("Delete roles error: %v", err)
	}

	// Insert user
	user := &User{Name: "Charlie", Email: "charlie10201@test.com", Id: 10201}
	err = db.User.Insert().One(user)
	if err != nil {
		t.Fatalf("Insert user error: %v", err)
	}

	// Insert roles and assign to user
	roleNames := []string{"Admin", "Editor", "Viewer"}
	for i, name := range roleNames {
		role := &Role{Name: name, Id: 10201 + i}
		err = db.Role.Insert().One(role)
		if err != nil {
			t.Fatalf("Insert role error: %v", err)
		}
		ur := &UserRole{UserId: 10201, RoleId: 10201 + i, Id: 10201 + i}
		err = db.UserRole.Insert().One(ur)
		if err != nil {
			t.Fatalf("Insert user role error: %v", err)
		}
	}

	// Use One() with With("UserRoles") to load the O2M relation
	result, err := db.User.Select().With("UserRoles").Filter(
		goent.Equals(db.User.Field("name"), "Charlie"),
	).One()
	if err != nil {
		t.Fatalf("Select One with UserRoles error: %v", err)
	}

	// BUG: Before fix, result.UserRoles was nil/empty because One() didn't load With relations
	if len(result.UserRoles) != len(roleNames) {
		t.Errorf("Expected %d UserRoles via One().With(), got %d (bug: One() ignores With())",
			len(roleNames), len(result.UserRoles))
	}
}

// TestOneWithM2OForeignLoading verifies One() loads M2O relations via With().
// UserRole has UserId and RoleId with `goe:"m2o"` tags.
// We test from the Role side: Role has UserRoles (O2M), and we use One()
// with With("UserRoles") to verify the O2M relation loads correctly.
func TestOneWithM2OForeignLoading(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Clean up
	err = db.UserRole.Delete().Exec()
	if err != nil {
		t.Fatalf("Delete user roles error: %v", err)
	}
	err = db.User.Delete().Exec()
	if err != nil {
		t.Fatalf("Delete users error: %v", err)
	}
	err = db.Role.Delete().Exec()
	if err != nil {
		t.Fatalf("Delete roles error: %v", err)
	}

	// Insert role
	role := &Role{Name: "SuperAdmin", Id: 10301}
	err = db.Role.Insert().One(role)
	if err != nil {
		t.Fatalf("Insert role error: %v", err)
	}

	// Insert users and assign them to the role
	userNames := []string{"Dave", "Eve", "Frank"}
	for i, name := range userNames {
		user := &User{Name: name, Email: fmt.Sprintf("%s10301@test.com", strings.ToLower(name)), Id: 10301 + i}
		err = db.User.Insert().One(user)
		if err != nil {
			t.Fatalf("Insert user error: %v", err)
		}
		ur := &UserRole{UserId: 10301 + i, RoleId: 10301, Id: 10301 + i}
		err = db.UserRole.Insert().One(ur)
		if err != nil {
			t.Fatalf("Insert user role error: %v", err)
		}
	}

	// Use One() with With("UserRoles") to load the O2M relation on Role
	result, err := db.Role.Select().With("UserRoles").Filter(
		goent.Equals(db.Role.Field("name"), "SuperAdmin"),
	).One()
	if err != nil {
		t.Fatalf("Select One with UserRoles error: %v", err)
	}

	// BUG: Before fix, result.UserRoles was nil/empty because One() didn't load With relations
	if len(result.UserRoles) != len(userNames) {
		t.Errorf("Expected %d UserRoles via One().With(), got %d (bug: One() ignores With())",
			len(userNames), len(result.UserRoles))
	}
}

// TestPaginationWithForeignLoading verifies that Pagination() correctly loads
// With() relations. Since Pagination() calls All() internally, and All() was
// already fixed, this test ensures the integration works end-to-end.
func TestPaginationWithForeignLoading(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Clean up
	err = db.UserRole.Delete().Exec()
	if err != nil {
		t.Fatalf("Delete user roles error: %v", err)
	}
	err = db.User.Delete().Exec()
	if err != nil {
		t.Fatalf("Delete users error: %v", err)
	}
	err = db.Role.Delete().Exec()
	if err != nil {
		t.Fatalf("Delete roles error: %v", err)
	}

	// Insert user with roles
	user := &User{Name: "Grace", Email: "grace10401@test.com", Id: 10401}
	err = db.User.Insert().One(user)
	if err != nil {
		t.Fatalf("Insert user error: %v", err)
	}

	roleNames := []string{"Admin", "Editor", "Viewer"}
	for i, name := range roleNames {
		role := &Role{Name: name, Id: 10401 + i}
		err = db.Role.Insert().One(role)
		if err != nil {
			t.Fatalf("Insert role error: %v", err)
		}
		ur := &UserRole{UserId: 10401, RoleId: 10401 + i, Id: 10401 + i}
		err = db.UserRole.Insert().One(ur)
		if err != nil {
			t.Fatalf("Insert user role error: %v", err)
		}
	}

	// Use Pagination with With("UserRoles")
	p, err := db.User.Select().With("UserRoles").Pagination(1, 10)
	if err != nil {
		t.Fatalf("Pagination with UserRoles error: %v", err)
	}

	if len(p.Values) == 0 {
		t.Fatal("Expected at least 1 user in pagination results")
	}

	var found *User
	for _, u := range p.Values {
		if u.Name == "Grace" {
			found = u
			break
		}
	}
	if found == nil {
		t.Fatal("Grace not found in pagination results")
	}

	if len(found.UserRoles) != len(roleNames) {
		t.Errorf("Expected %d UserRoles via Pagination().With(), got %d",
			len(roleNames), len(found.UserRoles))
	}
}

// TestFindForeignByNameCaseInsensitive verifies that With() accepts
// case-insensitive names. Before the fix, findForeignByName only did
// exact matching, so With("userroles") would fail when the relation was
// registered as "user_id" (derived from the FK column name).
func TestFindForeignByNameCaseInsensitive(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Clean up
	err = db.UserRole.Delete().Exec()
	if err != nil {
		t.Fatalf("Delete user roles error: %v", err)
	}
	err = db.User.Delete().Exec()
	if err != nil {
		t.Fatalf("Delete users error: %v", err)
	}
	err = db.Role.Delete().Exec()
	if err != nil {
		t.Fatalf("Delete roles error: %v", err)
	}

	// Insert test data
	user := &User{Name: "Heidi", Email: "heidi10501@test.com", Id: 10501}
	err = db.User.Insert().One(user)
	if err != nil {
		t.Fatalf("Insert user error: %v", err)
	}

	role := &Role{Name: "Tester", Id: 10501}
	err = db.Role.Insert().One(role)
	if err != nil {
		t.Fatalf("Insert role error: %v", err)
	}

	ur := &UserRole{UserId: 10501, RoleId: 10501, Id: 10501}
	err = db.UserRole.Insert().One(ur)
	if err != nil {
		t.Fatalf("Insert user role error: %v", err)
	}

	// Test With("userroles") with lowercase — should match case-insensitively
	users, err := db.User.Select().With("userroles").All()
	if err != nil {
		t.Fatalf("Select with lowercase 'userroles' error: %v", err)
	}

	var found *User
	for _, u := range users {
		if u.Name == "Heidi" {
			found = u
			break
		}
	}
	if found == nil {
		t.Fatal("Heidi not found")
	}

	if len(found.UserRoles) != 1 {
		t.Errorf("Expected 1 UserRole via With('userroles'), got %d (bug: case-sensitive findForeignByName)",
			len(found.UserRoles))
	}
}

// TestO2MRoleUserRolesQuery verifies O2M works from the Role side.
// Role has many UserRoles (O2M via fk=role_id).
func TestO2MRoleUserRolesQuery(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Clean up
	err = db.UserRole.Delete().Exec()
	if err != nil {
		t.Fatalf("Delete user roles error: %v", err)
	}
	err = db.User.Delete().Exec()
	if err != nil {
		t.Fatalf("Delete users error: %v", err)
	}
	err = db.Role.Delete().Exec()
	if err != nil {
		t.Fatalf("Delete roles error: %v", err)
	}

	// Insert role
	role := &Role{Name: "Developer", Id: 10601}
	err = db.Role.Insert().One(role)
	if err != nil {
		t.Fatalf("Insert role error: %v", err)
	}

	// Insert users and assign them to the role
	userNames := []string{"Ivan", "Judy", "Karl"}
	for i, name := range userNames {
		user := &User{Name: name, Email: fmt.Sprintf("%s10601@test.com", strings.ToLower(name)), Id: 10601 + i}
		err = db.User.Insert().One(user)
		if err != nil {
			t.Fatalf("Insert user error: %v", err)
		}
		ur := &UserRole{UserId: 10601 + i, RoleId: 10601, Id: 10601 + i}
		err = db.UserRole.Insert().One(ur)
		if err != nil {
			t.Fatalf("Insert user role error: %v", err)
		}
	}

	// Query role with UserRoles (O2M)
	roles, err := db.Role.Select().With("UserRoles").All()
	if err != nil {
		t.Fatalf("Select roles with UserRoles error: %v", err)
	}

	var found *Role
	for _, r := range roles {
		if r.Name == "Developer" {
			found = r
			break
		}
	}
	if found == nil {
		t.Fatal("Developer role not found")
	}

	// BUG: Before fix, only 1 UserRole was returned instead of 3
	if len(found.UserRoles) != len(userNames) {
		t.Errorf("Expected %d UserRoles for Developer, got %d (bug: O2M returns only 1 child)",
			len(userNames), len(found.UserRoles))
	}
}
