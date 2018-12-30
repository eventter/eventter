package main

import (
	"bytes"
	"encoding/xml"
	"fmt"
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
	for _, s := range r.Sections {
		s.Parent = r

		for _, t := range s.Types {
			t.Parent = s

			for _, encoding := range t.Encodings {
				encoding.Parent = t

				code, err := strconv.ParseInt(encoding.CodeHex[2:], 16, 32)
				if err != nil {
					return errors.Wrapf(err, "parse encoding code failed %s->%s->%s", s.Name, t.Name, encoding.CodeHex)
				}
				encoding.Code = int(code)
				encoding.CodeHex = ""
			}

			if t.Descriptor != nil {
				t.Descriptor.Parent = t

				parts := strings.Split(t.Descriptor.CodeHex, ":")
				if len(parts) != 2 {
					return errors.Errorf("bad descriptor code %s->%s", s.Name, t.Name)
				}
				high, err := strconv.ParseUint(parts[0][2:], 16, 32)
				if err != nil {
					return errors.Wrapf(err, "parse high descriptor code failed %s->%s", s.Name, t.Name)
				}
				low, err := strconv.ParseUint(parts[1][2:], 16, 32)
				if err != nil {
					return errors.Wrapf(err, "parse low descriptor code failed %s->%s", s.Name, t.Name)
				}
				t.Descriptor.Code = high<<32 | low
				t.Descriptor.CodeHex = ""
			}

			for _, field := range t.Fields {
				field.Parent = t
			}

			for _, choice := range t.Choices {
				choice.Parent = t
			}
		}

		for _, d := range s.Definitions {
			d.Parent = s
		}
	}

	return nil
}

func (r *Root) UnionTypeNames() []string {
	var names []string
	known := make(map[string]bool)

	for _, s := range r.Sections {
		for _, t := range s.Types {
			if t.Provides == "" {
				continue
			}

			for _, provides := range regexp.MustCompile(`,\s+`).Split(t.Provides, -1) {
				if provides == "source" || provides == "target" {
					continue
				}

				if provides == "frame" {
					provides = "amqp-frame"
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
	Parent      *Root         `xml:"-"`
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
	Parent     *Section    `xml:"-"`
}

func (t *Type) UnionTypeNames() []string {
	if t.Provides == "" {
		return nil
	}

	names := regexp.MustCompile(`,\s+`).Split(t.Provides, -1)
	for i, name := range names {
		if name == "frame" {
			names[i] = "amqp-frame"
		}
	}
	return names
}

func (t *Type) IsFrame() bool {
	for _, n := range t.UnionTypeNames() {
		if n == "amqp-frame" {
			return true
		}
		if n == "sasl-frame" {
			return true
		}
	}
	return false
}

func (t *Type) PrimitiveType() (*Type, error) {
	if t.Class == "primitive" {
		return t, nil
	} else if t.Class == "restricted" {
		for _, section := range t.Parent.Parent.Sections {
			for _, typ := range section.Types {
				if typ.Name == t.Source {
					return typ.PrimitiveType()
				}
			}
		}
		return nil, errors.Errorf("cannot find primitive type %s", t.Name)
	} else {
		return nil, errors.Errorf("%s is neither primitive, nor restricted type", t.Name)
	}
}

func (t *Type) GoType() (string, error) {
	root := t.Parent.Parent

	if t.Class == "composite" {
		return "*" + convert(t.Name), nil
	} else if t.Class != "primitive" {
		for _, section := range root.Sections {
			for _, typ := range section.Types {
				if typ.Name == t.Source {
					return typ.GoType()
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
	Parent   *Type  `xml:"-"`
}

type Descriptor struct {
	Name    string `xml:"name,attr"`
	Code    uint64 `xml:"-"`
	CodeHex string `xml:"code,attr"`
	Parent  *Type  `xml:"-"`
}

type Field struct {
	Name      string `xml:"name,attr"`
	TypeName  string `xml:"type,attr"`
	Requires  string `xml:"requires,attr"`
	Default   string `xml:"default"`
	Multiple  bool   `xml:"multiple,attr"`
	Mandatory bool   `xml:"mandatory,attr"`
	Parent    *Type  `xml:"-"`
}

func (f *Field) GoType() (string, error) {
	t, err := f.goType()
	if err != nil {
		return "", nil
	}
	if f.Multiple {
		return "[]" + t, nil
	}
	return t, nil
}

func (f *Field) Type() *Type {
	typeName := f.TypeName
	if typeName == "*" {
		if f.Requires == "source" || f.Requires == "target" {
			typeName = f.Requires
		} else {
			return &Type{
				Name:  f.Requires,
				Class: "union",
			}
		}
	}

	for _, s := range f.Parent.Parent.Parent.Sections {
		for _, t := range s.Types {
			if t.Name == typeName {
				return t
			}
		}
	}

	return nil
}

func (f *Field) UnionTypeName() string {
	t := f.Type()
	if t != nil {
		return t.Name
	}
	return ""
}

func (f *Field) TypeClass() string {
	t := f.Type()
	if t != nil {
		return t.Class
	}
	return ""
}

func (f *Field) PrimitiveTypeName() (string, error) {
	t := f.Type()
	if t.Class == "union" || t.Class == "composite" {
		return "", nil
	}
	if t == nil {
		return "", errors.Errorf("field %s (of type %s) couldn't find field type", f.Name, f.Parent.Name)
	}
	pt, err := t.PrimitiveType()
	if err != nil {
		return "", errors.Wrapf(err, "field %s (of type %s) could find field type's primitive type", f.Name, f.Parent.Name)
	}

	return pt.Name, nil

}

func (f *Field) goType() (string, error) {
	if f.TypeName == "*" {
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

	t := f.Type()
	if t == nil {
		return "", errors.Errorf("field %s (of type %s): did not find type %s", f.Name, f.Parent.Name, f.TypeName)
	}

	if t.Class == "primitive" {
		s, err := t.GoType()
		if err != nil {
			return "", errors.Wrapf(err, "field %s (of type %s)", f.Name, f.Parent.Name)
		}
		if s == "types.Struct" {
			return "*" + s, nil
		}
		return s, nil
	} else if t.Class == "composite" {
		return "*" + convert(f.TypeName), nil
	} else if t.Class == "restricted" {
		s, err := t.GoType()
		if err != nil {
			return "", errors.Wrapf(err, "field %s (of type %s)", f.Name, f.Parent.Name)
		}
		if s == "types.Struct" {
			return "*" + convert(f.TypeName), nil
		}
		return convert(f.TypeName), nil
	} else {
		return "", errors.Errorf("field %s (of type %s): class not handled %s", f.Name, f.Parent.Name, t.Class)
	}
}

func (f *Field) GoNonZeroCheck(expr string) (string, error) {
	if f.TypeName == "*" {
		return expr + " != nil", nil
	}

	if f.Multiple {
		return "len(" + expr + ") > 0", nil
	}

	t := f.Type()
	if t == nil {
		return "", errors.Errorf("field %s (of type %s): did not find type %s", f.Name, f.Parent.Name, f.TypeName)
	}

	var err error
	if t.Class == "restricted" {
		t, err = t.PrimitiveType()
		if err != nil {
			return "", errors.Wrapf(err, "field %s (of type %s): did not its primitive type %s", f.Name, f.Parent.Name, f.TypeName)
		}
	}

	if t.Class == "composite" {
		return expr + " != nil", nil
	} else if t.Class == "primitive" {
		switch t.Name {
		case "boolean":
			return expr + " != false", nil
		case "symbol":
			fallthrough
		case "string":
			return fmt.Sprintf("%s != %q", expr, ""), nil
		case "map":
			return expr + " != nil", nil
		case "binary":
			return expr + " != nil", nil
		case "timestamp":
			return fmt.Sprintf("!%s.IsZero()", expr), nil
		default:
			return expr + " != 0", nil
		}
	} else {
		return "", errors.Errorf("field %s (of type %s): unhandled class %s", f.Name, f.Parent.Name, t.Class)
	}
}

func (f *Field) AlwaysPresent() bool {
	if f.Mandatory {
		return true
	}

	found := false
	for _, sibling := range f.Parent.Fields {
		if sibling == f {
			found = true
		} else if found && sibling.Mandatory {
			return true
		}
	}

	return false
}

type Choice struct {
	Name   string `xml:"name,attr"`
	Value  string `xml:"value,attr"`
	Parent *Type  `xml:"-"`
}

type Definition struct {
	Name   string   `xml:"name,attr"`
	Value  string   `xml:"value,attr"`
	Parent *Section `xml:"-"`
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
		"hasPrefix": func(s, prefix string) bool {
			return strings.HasPrefix(s, prefix)
		},
		"inc": func(n int) int {
			return n + 1
		},
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
	"bytes"
	"encoding/hex"
	"math"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
)

const (
	DescriptorEncoding = 0x00
)

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

type DescribedType interface {
	Descriptor() uint64
}

type BufferMarshaler interface {
	MarshalBuffer(buf *bytes.Buffer) error
}

type BufferUnmarshaler interface {
	UnmarshalBuffer(buf *bytes.Buffer) error
}

type Frame interface {
	GetFrameMeta() *FrameMeta
	DescribedType
	BufferMarshaler
	BufferUnmarshaler
}

type FrameMeta struct {
	Size uint32
	DataOffset uint8
	Type uint8
	Channel uint16
	Payload []byte
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
					{{ if $type.IsFrame }}
						FrameMeta
					{{- end -}}
					{{ range $field := $type.Fields }}
						{{ $field.Name | convert }} {{ $field.GoType }}
					{{- end }}
				}

				{{ range $name := $type.UnionTypeNames }}
					func (*{{ $goTypeName }}) is{{ $name | convert }}() {}
				{{ end }}

				{{ if $type.IsFrame }}
					func (t *{{ $goTypeName}}) GetFrameMeta() *FrameMeta {
						return &t.FrameMeta
					}
				{{ end }}

				{{ with $type.Descriptor }}
					func (t *{{ $goTypeName}}) Descriptor() uint64 {
						return {{ $goTypeName }}Descriptor
					}
				{{ end }}

				func (t *{{ $goTypeName }}) Marshal() ([]byte, error) {
					buf := bytes.Buffer{}
					err := t.MarshalBuffer(&buf)
					if err != nil {
						return nil, err
					}
					return buf.Bytes(), nil
				}

				func (t *{{ $goTypeName }}) MarshalBuffer(buf *bytes.Buffer) (err error) {
					var count uint32 = 0
					{{ range $index, $field := $type.Fields -}}
						{{- if $field.Mandatory -}}
							count = {{ inc $index }} // {{ $field.Name }} is mandatory
						{{- else if $field.AlwaysPresent -}}
							count = {{ inc $index }} // {{ $field.Name }} precedes mandatory field(s), must be always present
						{{- else -}}
							if {{ $field.GoNonZeroCheck (join "t." (convert $field.Name)) }} { count = {{ inc $index }} }
						{{- end }}
					{{ end }}

					if count == 0 {
						buf.WriteByte(List0Encoding)
					} else {
						if count <= math.MaxUint8 {
							buf.WriteByte(List8Encoding)
							buf.WriteByte(byte(count))
						} else {
							var x [4] byte
							buf.WriteByte(List32Encoding)
							endian.PutUint32(x[:], count)
							buf.Write(x[:])
						}

						{{ range $index, $field := $type.Fields -}}
							if count > {{ $index }} {
							{{- $fieldClass := $field.TypeClass -}}
							{{ if $field.Multiple -}}
								{{ if or (eq $fieldClass "primitive") (eq $fieldClass "restricted") -}}
									err = marshal{{ $field.TypeName | convert }}Array(t.{{ $field.Name | convert }}, buf)
									if err != nil {
										return errors.Wrap(err, "marshal field {{ $field.Name }} failed")
									}
								{{ else }}
									panic("implement me: marshal multiple {{ $fieldClass }}")
								{{- end }}
							{{- else if eq $fieldClass "primitive" -}}
								err = marshal{{ $field.TypeName | convert }}(t.{{ $field.Name | convert }}, buf)
								if err != nil {
									return errors.Wrap(err, "marshal field {{ $field.Name }} failed")
								}
							{{- else if or (eq $fieldClass "restricted") (eq $fieldClass "composite") -}}
								err = t.{{ $field.Name | convert }}.MarshalBuffer(buf)
								if err != nil {
									return errors.Wrap(err, "marshal field {{ $field.Name }} failed")
								}
							{{- else if eq $fieldClass "union" -}}
								err = marshal{{ $field.UnionTypeName | convert }}Union(t.{{ $field.Name | convert }}, buf)
								if err != nil {
									return errors.Wrap(err, "marshal field {{ $field.Name }} failed")
								}
							{{- else -}}
								panic("implement me: marshal {{ $fieldClass }} field")
							{{- end }}
						{{ end }}
						{{- range $type.Fields }}
							}
						{{- end }}
					}
					return nil
				}

				func (t *{{ $goTypeName }}) Unmarshal(data []byte) error {
					return t.UnmarshalBuffer(bytes.NewBuffer(data))
				}

				func (t *{{ $goTypeName }}) UnmarshalBuffer(buf *bytes.Buffer) error {
					constructor, err := buf.ReadByte()
					if err != nil {
						return errors.Wrap(err, "read constructor failed")
					}
					var (
						count uint32
						done uint32
					)
					switch constructor {
					case NullEncoding:
						fallthrough
					case List0Encoding:
						return nil
					case List8Encoding:
						v, err := buf.ReadByte()
						if err != nil {
							return errors.Wrap(err, "read length failed")
						}
						count = uint32(v)
					case List32Encoding:
						if buf.Len() < 4 {
							return errors.New("read length failed: buffer underflow")
						}
						count = endian.Uint32(buf.Next(4))
					}

					{{ range $index, $field := $type.Fields -}}
						if count > {{ $index }} {
						{{- $fieldClass := $field.TypeClass }}
						{{- if or (ne $fieldClass "restricted") (eq $field.PrimitiveTypeName "map") $field.Multiple }}
							constructor, err = buf.ReadByte()
							if err != nil {
								return errors.Wrap(err, "unmarshal field {{ $field.Name }} failed")
							}
						{{- end }}
						{{ if $field.Multiple -}}
							{{ if or (eq $fieldClass "primitive") (eq $fieldClass "restricted") -}}
								err = unmarshal{{ $field.TypeName | convert }}Array(&t.{{ $field.Name | convert }}, constructor, buf)
								if err != nil {
									return errors.Wrap(err, "unmarshal field {{ $field.Name }} failed")
								}
							{{ else }}
								panic("implement me: unmarshal multiple {{ $fieldClass }}")
							{{- end }}
						{{- else if eq $field.PrimitiveTypeName "map" -}}
							var map{{ $index }} *types.Struct
							err = unmarshalMap(&map{{ $index }}, constructor, buf)
							if err != nil {
								return errors.Wrap(err, "unmarshal field {{ $field.Name }} failed")
							}
							t.{{ $field.Name | convert }} = ({{ $field.GoType }})(map{{ $index }})
						{{- else if eq $fieldClass "primitive" -}}
							err = unmarshal{{ $field.TypeName | convert }}(&t.{{ $field.Name | convert }}, constructor, buf)
							if err != nil {
								return errors.Wrap(err, "unmarshal field {{ $field.Name }} failed")
							}
						{{- else if or (eq $fieldClass "restricted") (eq $fieldClass "composite") -}}
							err = t.{{ $field.Name | convert }}.UnmarshalBuffer(buf)
							if err != nil {
								return errors.Wrap(err, "unmarshal field {{ $field.Name }} failed")
							}
						{{- else if eq $fieldClass "union" -}}
							err = unmarshal{{ $field.UnionTypeName | convert }}Union(&t.{{ $field.Name | convert }}, constructor, buf)
							if err != nil {
								return errors.Wrap(err, "unmarshal field {{ $field.Name }} failed")
							}
						{{- else -}}
							panic("implement me: unmarshal {{ $fieldClass }} field")
						{{- end }}
							done = {{ inc $index }}
					{{ end }}
					{{- range $type.Fields }}
						}
					{{- end }}

					if count > done {
						return errors.New("unmarshal failed: some fields were not read")
					}

					return nil
				}

			{{ else if eq $type.Class "restricted" }}
				type {{ $goTypeName }} {{ $type.GoType }}
				{{ with $type.Choices }}
					const (
						{{ range $choice := $type.Choices }}
							{{ $goType := $type.GoType -}}
							{{ joinWith "-" $choice.Name $type.Name | convert }} {{ $goTypeName }} = {{ if or (eq $goType "string") }}{{ printf "%q" $choice.Value }}{{ else }}{{ $choice.Value }}{{ end }}
						{{- end }}
					)

					{{ if eq $type.GoType "string" }}
						func (t {{ $goTypeName }}) String() string {
							return string(t)
						}
					{{ else }}
						func (t {{ $goTypeName }}) String() string {
							switch t {
							{{ range $choice := $type.Choices -}}
							case {{ joinWith "-" $choice.Name $type.Name | convert }}:
								return {{ printf "%q" $choice.Name }}
							{{ end -}}
							default:
								return "<invalid>"
							}
						}
					{{ end }}
				{{ end }}

				{{ range $name := $type.UnionTypeNames }}
					func ({{ $goTypeName }}) is{{ $name | convert }}() {}
				{{ end }}

				{{ $primitiveType := $type.PrimitiveType -}}

				{{ if eq $primitiveType.Name "map" }}
					func (t *{{ $goTypeName }}) Marshal() ([]byte, error) {
						buf := bytes.Buffer{}
						err := t.MarshalBuffer(&buf)
						if err != nil {
							return nil, err
						}
						return buf.Bytes(), nil
					}

					func (t *{{ $goTypeName }}) MarshalBuffer(buf *bytes.Buffer) error {
						return marshalMap((*types.Struct)(t), buf)
					}

					func (t *{{ $goTypeName }}) Unmarshal(data []byte) error {
						return t.UnmarshalBuffer(bytes.NewBuffer(data))
					}

					func (t *{{ $goTypeName }}) UnmarshalBuffer(buf *bytes.Buffer) error {
						constructor, err := buf.ReadByte()
						if err != nil {
							return errors.Wrap(err, "read constructor failed")
						}

						var m *types.Struct
						err = unmarshalMap(&m, constructor, buf)
						if err != nil {
							return err
						}

						*t = ({{ $goTypeName }})(*m)
						return nil
					}

				{{ else }}
					func (t {{ $goTypeName }}) Marshal() ([]byte, error) {
						buf := bytes.Buffer{}
						err := t.MarshalBuffer(&buf)
						if err != nil {
							return nil, err
						}
						return buf.Bytes(), nil
					}

					func (t {{ $goTypeName }}) MarshalBuffer(buf *bytes.Buffer) error {
						return marshal{{ $primitiveType.Name | convert }}({{ $type.GoType }}(t), buf)
					}

					func (t *{{ $goTypeName }}) Unmarshal(data []byte) error {
						return t.UnmarshalBuffer(bytes.NewBuffer(data))
					}

					func (t *{{ $goTypeName }}) UnmarshalBuffer(buf *bytes.Buffer) error {
						{{ $primitiveType := $type.PrimitiveType -}}
						constructor, err := buf.ReadByte()
						if err != nil {
							return errors.Wrap(err, "read constructor failed")
						}
						return unmarshal{{ $primitiveType.Name | convert }}((*{{ $type.GoType }})(t), constructor, buf)
					}
				{{ end }}

			{{ else if eq $type.Class "primitive" }}
				const (
					{{- range $encoding := $type.Encodings }}
						{{- if hasPrefix $encoding.Name $type.Name }}
							{{ $encoding.Name | convert }}Encoding = {{ printf "0x%02x" $encoding.Code }}
						{{- else }}
							{{ joinWith "-" $type.Name $encoding.Name | convert }}Encoding = {{ printf "0x%02x" $encoding.Code }}
						{{- end }}
					{{- end }}
				)
			{{ end }}
		{{ end }}
	{{ end }}
{{ end }}

{{- end -}}
`
