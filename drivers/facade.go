package drivers

import (
	"strings"

	"github.com/azhai/goent"
	"github.com/azhai/goent/drivers/pgsql"
	"github.com/azhai/goent/drivers/sqlite"
	"github.com/azhai/goent/model"
	"github.com/azhai/goent/utils"
)

// QuickOpen opens a database connection with default configuration
func QuickOpen[T any](dbType, dbDSN, logFile string) (*T, error) {
	drv, err := OpenDSN(dbType, dbDSN)
	if err != nil {
		return nil, err
	}
	if logFile != "" {
		err = drv.AddLogger(utils.CreateLogger(logFile))
		if err != nil {
			return nil, err
		}
	}
	return goent.Open[T](drv)
}

// OpenDSN opens a database connection with the given type and DSN
func OpenDSN(dbType, dbDSN string) (drv model.Driver, err error) {
	dbType = strings.ToLower(dbType)
	if dbType == "pgsql" || dbType == "postgres" {
		drv = pgsql.OpenDSN(dbDSN)
	} else if dbType == "" && strings.HasPrefix(dbDSN, "postgres://") {
		drv = pgsql.OpenDSN(dbDSN)
	} else {
		err = utils.MakeDirForFile(dbDSN)
		drv = sqlite.OpenDSN(dbDSN)
	}
	return
}
