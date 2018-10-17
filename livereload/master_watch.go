//+build !production

package livereload

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/tools/go/packages"
)

func (m *master) watchPackage(path string) error {
	pkgs, err := packages.Load(&packages.Config{Mode: packages.LoadImports}, path)
	if err != nil || len(pkgs) != 1 {
		return errors.Wrapf(err, "loading package [%s] failed", path)
	}

	pkg := pkgs[0]
	goroot := runtime.GOROOT()

	processed := make(map[string]bool)
	processed[pkg.ID] = true
	processed["C"] = true
	processed["unsafe"] = true
	queue := []*packages.Package{pkg}

	for ; len(queue) > 0; queue = queue[1:] {
		pkg := queue[0]
		pkgDir := getPackageDir(pkg)
		if pkgDir == "" {
			return fmt.Errorf("could not determine package directory of [%s]", pkg.ID)
		} else if strings.HasPrefix(pkgDir, goroot) {
			// skip stdlib packages
			continue
		}

		for _, importedPkg := range pkg.Imports {
			if processed[importedPkg.ID] {
				continue
			}
			processed[importedPkg.ID] = true

			importedPkgDir := getPackageDir(importedPkg)
			if importedPkgDir == "" {
				return fmt.Errorf("could not determine package dir of [%s]", pkg.ID)
			} else if strings.HasPrefix(importedPkgDir, goroot) {
				continue
			}

			queue = append(queue, importedPkg)
		}

		m.watchedPackageDirs[pkgDir] = pkg.ID
		m.watcher.Add(pkgDir)
	}

	return nil
}

func getPackageDir(pkg *packages.Package) string {
	if len(pkg.GoFiles) > 0 {
		return filepath.Dir(pkg.GoFiles[0])
	}

	if len(pkg.OtherFiles) > 0 {
		return filepath.Dir(pkg.OtherFiles[0])
	}

	return ""
}
