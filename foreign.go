package goent

const (
	_ ForeignType = iota
	O2O
	O2M
	M2O
	M2M
)

type ForeignType uint

type ThirdParty struct {
	Table       string
	Left, Right string
	Where       Condition
}

type Foreign struct {
	Type       ForeignType
	MountField string
	ForeignKey string
	Reference  *Field
	Middle     *ThirdParty
	Where      Condition
}
