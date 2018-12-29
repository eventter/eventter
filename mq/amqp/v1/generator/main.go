package main

import (
	"bytes"
	"encoding/xml"
	"go/format"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"text/template"

	"github.com/pkg/errors"
)

type Root struct {
	OutputFilename string
	InputFilenames []string
	Sections       []*Section `xml:"section"`
}

func (r *Root) Merge(other *Root) {
	r.Sections = append(r.Sections, other.Sections...)
}

func (r *Root) Section(name string) *Section {
	for _, section := range r.Sections {
		if section.Name == name {
			return section
		}
	}
	return nil
}

func (r *Root) Init() error {
	for _, section := range r.Sections {
		for _, typ := range section.Types {
			for _, encoding := range typ.Encodings {
				code, err := strconv.ParseInt(encoding.CodeHex[2:], 16, 32)
				if err != nil {
					return errors.Wrapf(err, "parse encoding code failed %s->%s->%s", section.Name, typ.Name, encoding.CodeHex)
				}
				encoding.Code = int(code)
				encoding.CodeHex = ""
			}
			if typ.Descriptor != nil {
				parts := strings.Split(typ.Descriptor.CodeHex, ":")
				if len(parts) != 2 {
					return errors.Errorf("bad descriptor code %s->%s", section.Name, typ.Name)
				}
				high, err := strconv.ParseUint(parts[0][2:], 16, 32)
				if err != nil {
					return errors.Wrapf(err, "parse high descriptor code failed %s->%s", section.Name, typ.Name)
				}
				low, err := strconv.ParseUint(parts[1][2:], 16, 32)
				if err != nil {
					return errors.Wrapf(err, "parse low descriptor code failed %s->%s", section.Name, typ.Name)
				}
				typ.Descriptor.Code = high<<32 | low
				typ.Descriptor.CodeHex = ""
			}
			for _, field := range typ.Fields {
				field.Parent = typ
			}
		}
	}

	return nil
}

func (r *Root) UnionTypeNames() []string {
	var names []string
	known := make(map[string]bool)

	for _, section := range r.Sections {
		for _, typ := range section.Types {
			if typ.Provides == "" {
				continue
			}

			for _, provides := range regexp.MustCompile(`,\s+`).Split(typ.Provides, -1) {
				if provides == "source" || provides == "target" {
					continue
				}

				if !known[provides] {
					names = append(names, provides)
					known[provides] = true
				}
			}
		}
	}

	return names
}

type Section struct {
	Name        string        `xml:"name,attr"`
	Types       []*Type       `xml:"type"`
	Definitions []*Definition `xml:"definition"`
}

type Type struct {
	Name       string      `xml:"name,attr"`
	Class      string      `xml:"class,attr"`
	Source     string      `xml:"source,attr"`
	Provides   string      `xml:"provides,attr"`
	Encodings  []*Encoding `xml:"encoding"`
	Descriptor *Descriptor `xml:"descriptor"`
	Fields     []*Field    `xml:"field"`
	Choices    []*Choice   `xml:"choice"`
}

func (t *Type) UnionTypeNames() []string {
	if t.Provides == "" {
		return nil
	}

	return regexp.MustCompile(`,\s+`).Split(t.Provides, -1)
}

func (t *Type) GoType(root *Root, path ...string) (string, error) {
	path = append(path, t.Name)

	if t.Class == "composite" {
		return "*" + convert(t.Name), nil
	} else if t.Class != "primitive" {
		for _, section := range root.Sections {
			for _, typ := range section.Types {
				if typ.Name == t.Source {
					return typ.GoType(root, path...)
				}
			}
		}

		return "", errors.Errorf("cannot find type %s", t.Name)

	}

	switch t.Name {
	case "null":
		return "Null", nil
	case "boolean":
		return "bool", nil
	case "ubyte":
		return "uint8", nil
	case "ushort":
		return "uint16", nil
	case "uint":
		return "uint32", nil
	case "ulong":
		return "uint64", nil
	case "byte":
		return "int8", nil
	case "short":
		return "int16", nil
	case "int":
		return "int32", nil
	case "long":
		return "int64", nil
	case "float":
		return "float32", nil
	case "double":
		return "float64", nil
	case "decimal32":
		fallthrough
	case "decimal64":
		fallthrough
	case "decimal128":
		return "", errors.New("decimals not implemented")
	case "char":
		return "rune", nil
	case "timestamp":
		return "time.Time", nil
	case "uuid":
		return "UUID", nil
	case "binary":
		return "[]byte", nil
	case "string":
		fallthrough
	case "symbol":
		return "string", nil
	case "map":
		return "types.Struct", nil
	case "list":
		return "list", nil
	default:
		return "", errors.Errorf("primitive type %s not handled", t.Name)
	}
}

type Encoding struct {
	Name     string `xml:"name,attr"`
	Code     int    `xml:"-"`
	CodeHex  string `xml:"code,attr" json:"-"`
	Category string `xml:"category,attr"`
	Width    int    `xml:"width,attr"`
}

type Descriptor struct {
	Name    string `xml:"name,attr"`
	Code    uint64 `xml:"-"`
	CodeHex string `xml:"code,attr"`
}

type Field struct {
	Name      string `xml:"name,attr"`
	Type      string `xml:"type,attr"`
	Requires  string `xml:"requires,attr"`
	Default   string `xml:"default"`
	Multiple  bool   `xml:"multiple,attr"`
	Mandatory bool   `xml:"mandatory,attr"`
	Parent    *Type  `xml:"-"`
}

func (f *Field) GoType(root *Root) (string, error) {
	t, err := f.goType(root)
	if err != nil {
		return "", nil
	}
	if f.Multiple {
		return "[]" + t, nil
	}
	return t, nil
}

func (f *Field) goType(root *Root) (string, error) {
	if f.Type == "*" {
		if f.Requires == "" {
			return "", errors.Errorf("field %s (of type %s): empty requires", f.Name, f.Parent.Name)
		}

		if f.Requires == "source" {
			return "*Source", nil
		} else if f.Requires == "target" {
			return "*Target", nil
		} else {
			return convert(f.Requires), nil
		}
	}

	for _, section := range root.Sections {
		for _, typ := range section.Types {
			if typ.Name != f.Type {
				continue
			}

			if typ.Class == "primitive" {
				s, err := typ.GoType(root)
				if err != nil {
					return "", errors.Wrapf(err, "field %s (of type %s)", f.Name, f.Parent.Name)
				}
				if s == "types.Struct" {
					return "*" + s, nil
				}
				return s, nil
			} else if typ.Class == "composite" {
				return "*" + convert(f.Type), nil
			} else if typ.Class == "restricted" {
				s, err := typ.GoType(root)
				if err != nil {
					return "", errors.Wrapf(err, "field %s (of type %s)", f.Name, f.Parent.Name)
				}
				if s == "types.Struct" {
					return "*" + convert(f.Type), nil
				}
				return convert(f.Type), nil
			} else {
				return "", errors.Errorf("field %s (of type %s): class not handled %s", f.Name, f.Parent.Name, typ.Class)
			}
		}
	}

	return "", errors.Errorf("field %s (of type %s): did not find type %s", f.Name, f.Parent.Name, f.Type)
}

type Choice struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}

type Definition struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func convert(s string) string {
	parts := strings.Split(s, "-")
	for i, part := range parts {
		part = strings.ToLower(part)

		if part == "id" || part == "uuid" || part == "sasl" || part == "tls" || part == "amqp" || part == "ietf" {
			parts[i] = strings.ToUpper(part)
		} else if len(part) > 1 {
			parts[i] = strings.ToUpper(part[:1]) + part[1:]
		} else {
			parts[i] = strings.ToUpper(part)
		}
	}
	return strings.Join(parts, "")
}

func run() error {
	root := &Root{
		OutputFilename: os.Args[1],
		InputFilenames: os.Args[2:],
	}

	for _, filename := range root.InputFilenames {
		data, err := ioutil.ReadFile(filename)
		if err != nil {
			return errors.Wrap(err, "read failed")
		}

		fileRoot := &Root{}

		if err := xml.Unmarshal(data, fileRoot); err != nil {
			return errors.Wrap(err, "unmarshal failed")
		}

		root.Merge(fileRoot)
	}

	err := root.Init()
	if err != nil {
		return errors.Wrap(err, "init failed")
	}

	buffer := bytes.Buffer{}

	tpl := template.New("tpl").Funcs(map[string]interface{}{
		"join": func(s ...string) string {
			return strings.Join(s, "")
		},
		"joinWith": func(sep string, s ...string) string {
			return strings.Join(s, sep)
		},
		"convert": convert,
	})
	tpl, err = tpl.Parse(templateText)
	if err != nil {
		return errors.Wrap(err, "template parse failed")
	}

	if err := tpl.Execute(&buffer, root); err != nil {
		return errors.Wrap(err, "template execute failed")
	}

	output, err := format.Source(buffer.Bytes())
	if err != nil {
		os.Stdout.Write(buffer.Bytes())
		return errors.Wrap(err, "format failed")
	}

	if err := ioutil.WriteFile(root.OutputFilename, output, 0644); err != nil {
		return errors.Wrap(err, "write failed")
	}

	return nil
}

const templateText = `
{{- block "root" . -}}
{{- $root := . -}}
// Code generated by ./generator/main.go. DO NOT EDIT.
package v1

//go:generate go run ./generator {{ .OutputFilename }} {{ range .InputFilenames }} {{ . }}{{ end }}

import (
	"encoding/hex"
	"time"

	"github.com/gogo/protobuf/types"
)

var _ = time.Time{}
var _ = types.Struct{}

type UUID [16]byte

func (u UUID) String() string {
	var x [36]byte
	hex.Encode(x[:8], u[:4])
	x[8] = '-'
	hex.Encode(x[9:13], u[4:6])
	x[13] = '-'
	hex.Encode(x[14:18], u[6:8])
	x[18] = '-'
	hex.Encode(x[19:23], u[8:10])
	x[23] = '-'
	hex.Encode(x[24:], u[10:])
	return string(x[:])
}

{{ range $name := .UnionTypeNames }}
type {{ $name | convert }} interface {
	is{{ $name | convert }}()
}
{{ end }}

{{ range $section := .Sections }}
	{{ with $section.Definitions }}
		const (
			{{ range $definition := $section.Definitions }}
				{{ if ne $definition.Name "MESSAGE-FORMAT" -}}
					{{ $definition.Name | convert }} = {{ $definition.Value }}
				{{- end }}
			{{- end }}
		)
	{{ end }}
	{{ range $type := $section.Types }}
		{{ if and (ne $type.Name "amqp-value") (ne $type.Name "amqp-sequence") }}
			{{ $goTypeName := $type.Name | convert }}
			{{ with $type.Descriptor }}
				const (
					{{ $goTypeName }}Name = {{ printf "%q" $type.Descriptor.Name }}
					{{ $goTypeName }}Descriptor = {{ printf "0x%016x" $type.Descriptor.Code }}
				)
			{{ end }}
			{{ if eq $type.Class "composite" }}
				type {{ $goTypeName }} struct {
					{{ range $field := $type.Fields }}
						{{ $field.Name | convert }} {{ $field.GoType $root }}
					{{- end }}
				}

				{{ range $name := $type.UnionTypeNames }}
					func (*{{ $goTypeName }}) is{{ $name | convert }}() {}
				{{ end }}

				func (f *{{ $goTypeName }}) Marshal() ([]byte, error) {
					panic("implement me")
				}

				func (f *{{ $goTypeName }}) Unmarshal(data []byte) error {
					panic("implement me")
				}

			{{ else if eq $type.Class "restricted" }}
				type {{ $goTypeName }} {{ $type.GoType $root }}
				{{ with $type.Choices }}
					const (
						{{ range $choice := $type.Choices }}
							{{ $goType := $type.GoType $root -}}
							{{ joinWith "-" $choice.Name $type.Name | convert }} {{ $goTypeName }} = {{ if or (eq $goType "string") }}{{ printf "%q" $choice.Value }}{{ else }}{{ $choice.Value }}{{ end }}
						{{- end }}
					)
				{{ end }}

				{{ range $name := $type.UnionTypeNames }}
					func ({{ $goTypeName }}) is{{ $name | convert }}() {}
				{{ end }}
			{{ else if eq $type.Class "primitive" }}
				const (
					{{- range $encoding := $type.Encodings }}
						{{ joinWith "-" $type.Name $encoding.Name | convert }}Encoding = {{ printf "0x%02x" $encoding.Code }}
					{{- end }}
				)
			{{ end }}
		{{ end }}
	{{ end }}
{{ end }}

{{- end -}}
`
