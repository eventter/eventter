package emq

import (
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

var (
	nameRegex          = regexp.MustCompile("^[a-zA-Z_][0-9a-zA-Z-_]*$")
	reservedNameRegex  = regexp.MustCompile("^_")
	validExchangeTypes = map[string]bool{
		ExchangeTypeDirect:  true,
		ExchangeTypeFanout:  true,
		ExchangeTypeTopic:   true,
		ExchangeTypeHeaders: true,
	}
)

const (
	blankErrorFormat        = "%s cannot be blank"
	nameInvalidErrorFormat  = "%s must begin with letter or underscore and contain only letters, numbers, hyphens & underscores"
	reservedNameErrorFormat = "%s beginning with underscore is reserved"
	stringLengthErrorFormat = "%s must be at most %d characters long"
	listErrorFormat         = "%s %s is not valid"
	negativeErrorFormat     = "%s must not be negative"
	nameMaxLength           = 64
)

type RequestValidationError struct {
	Errors []error
}

func (e *RequestValidationError) Error() string {
	s := "there are following validation errors:\n"
	for _, err := range e.Errors {
		s += "- " + err.Error() + "\n"
	}
	return strings.TrimRight(s, "\n")
}

func (r *NamespaceCreateRequest) Validate() error {
	var errs []error

	if r.Namespace == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "namespace"))
	} else if !nameRegex.MatchString(r.Namespace) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "namespace"))
	} else if reservedNameRegex.MatchString(r.Namespace) {
		errs = append(errs, errors.Errorf(reservedNameErrorFormat, "namespace"))
	} else if len(r.Namespace) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "namespace", nameMaxLength))
	}

	if len(errs) > 0 {
		return &RequestValidationError{errs}
	}

	return nil
}

func (r *NamespaceDeleteRequest) Validate() error {
	var errs []error

	if r.Namespace == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "namespace"))
	} else if !nameRegex.MatchString(r.Namespace) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "namespace"))
	} else if reservedNameRegex.MatchString(r.Namespace) {
		errs = append(errs, errors.Errorf(reservedNameErrorFormat, "namespace"))
	} else if len(r.Namespace) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "namespace", nameMaxLength))
	} else if r.Namespace == DefaultNamespace {
		errs = append(errs, errors.New("default namespace cannot be deleted"))
	}

	if len(errs) > 0 {
		return &RequestValidationError{errs}
	}

	return nil
}

func (r *TopicCreateRequest) Validate() error {
	var errs []error

	if r.Topic.Name.Namespace == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "namespace"))
	} else if !nameRegex.MatchString(r.Topic.Name.Namespace) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "namespace"))
	} else if reservedNameRegex.MatchString(r.Topic.Name.Namespace) {
		errs = append(errs, errors.Errorf(reservedNameErrorFormat, "namespace"))
	} else if len(r.Topic.Name.Namespace) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "namespace", nameMaxLength))
	}

	if r.Topic.Name.Name == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "topic name"))
	} else if !nameRegex.MatchString(r.Topic.Name.Name) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "topic name"))
	} else if reservedNameRegex.MatchString(r.Topic.Name.Name) {
		errs = append(errs, errors.Errorf(reservedNameErrorFormat, "topic name"))
	} else if len(r.Topic.Name.Name) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "topic name", nameMaxLength))
	}

	if r.Topic.DefaultExchangeType == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "exchange type"))
	} else if !validExchangeTypes[r.Topic.DefaultExchangeType] {
		errs = append(errs, errors.Errorf(listErrorFormat, "exchange type", r.Topic.DefaultExchangeType))
	}

	if r.Topic.Retention < 0 {
		errs = append(errs, errors.Errorf(negativeErrorFormat, "retention"))
	}

	if len(errs) > 0 {
		return &RequestValidationError{errs}
	}

	return nil
}

func (r *TopicListRequest) Validate() error {
	var errs []error

	if r.Topic.Namespace == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "namespace"))
	} else if !nameRegex.MatchString(r.Topic.Namespace) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "namespace"))
	} else if reservedNameRegex.MatchString(r.Topic.Namespace) {
		errs = append(errs, errors.Errorf(reservedNameErrorFormat, "namespace"))
	} else if len(r.Topic.Namespace) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "namespace", nameMaxLength))
	}

	if r.Topic.Name != "" {
		if !nameRegex.MatchString(r.Topic.Name) {
			errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "topic name"))
		} else if reservedNameRegex.MatchString(r.Topic.Name) {
			errs = append(errs, errors.Errorf(reservedNameErrorFormat, "topic name"))
		} else if len(r.Topic.Name) > nameMaxLength {
			errs = append(errs, errors.Errorf(stringLengthErrorFormat, "topic name", nameMaxLength))
		}
	}

	if len(errs) > 0 {
		return &RequestValidationError{errs}
	}

	return nil
}

func (r *TopicDeleteRequest) Validate() error {
	var errs []error

	if r.Topic.Namespace == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "namespace"))
	} else if !nameRegex.MatchString(r.Topic.Namespace) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "namespace"))
	} else if reservedNameRegex.MatchString(r.Topic.Namespace) {
		errs = append(errs, errors.Errorf(reservedNameErrorFormat, "namespace"))
	} else if len(r.Topic.Namespace) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "namespace", nameMaxLength))
	}

	if r.Topic.Name == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "topic name"))
	} else if !nameRegex.MatchString(r.Topic.Name) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "topic name"))
	} else if reservedNameRegex.MatchString(r.Topic.Name) {
		errs = append(errs, errors.Errorf(reservedNameErrorFormat, "topic name"))
	} else if len(r.Topic.Name) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "topic name", nameMaxLength))
	}

	if len(errs) > 0 {
		return &RequestValidationError{errs}
	}

	return nil
}

func (r *ConsumerGroupCreateRequest) Validate() error {
	var errs []error

	if r.ConsumerGroup.Name.Namespace == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "namespace"))
	} else if !nameRegex.MatchString(r.ConsumerGroup.Name.Namespace) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "namespace"))
	} else if reservedNameRegex.MatchString(r.ConsumerGroup.Name.Namespace) {
		errs = append(errs, errors.Errorf(reservedNameErrorFormat, "namespace"))
	} else if len(r.ConsumerGroup.Name.Namespace) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "namespace", nameMaxLength))
	}

	if r.ConsumerGroup.Name.Name == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "consumer group name"))
	} else if !nameRegex.MatchString(r.ConsumerGroup.Name.Name) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "consumer group name"))
	} else if reservedNameRegex.MatchString(r.ConsumerGroup.Name.Name) {
		errs = append(errs, errors.Errorf(reservedNameErrorFormat, "consumer group name"))
	} else if len(r.ConsumerGroup.Name.Name) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "consumer group name", nameMaxLength))
	}

	for i, clientBinding := range r.ConsumerGroup.Bindings {
		if clientBinding.ExchangeType == "" {
			errs = append(errs, errors.Wrapf(errors.Errorf(blankErrorFormat, "exchange type"), "binding %d", i))
		} else if !validExchangeTypes[clientBinding.ExchangeType] {
			errs = append(errs, errors.Wrapf(errors.Errorf(listErrorFormat, "exchange type", clientBinding.ExchangeType), "binding %d", i))
		}
	}

	if len(errs) > 0 {
		return &RequestValidationError{errs}
	}

	return nil
}

func (r *ConsumerGroupListRequest) Validate() error {
	var errs []error

	if r.ConsumerGroup.Namespace == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "namespace"))
	} else if !nameRegex.MatchString(r.ConsumerGroup.Namespace) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "namespace"))
	} else if reservedNameRegex.MatchString(r.ConsumerGroup.Namespace) {
		errs = append(errs, errors.Errorf(reservedNameErrorFormat, "namespace"))
	} else if len(r.ConsumerGroup.Namespace) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "namespace", nameMaxLength))
	}

	if r.ConsumerGroup.Name != "" {
		if !nameRegex.MatchString(r.ConsumerGroup.Name) {
			errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "consumer group name"))
		} else if reservedNameRegex.MatchString(r.ConsumerGroup.Name) {
			errs = append(errs, errors.Errorf(reservedNameErrorFormat, "consumer group name"))
		} else if len(r.ConsumerGroup.Name) > nameMaxLength {
			errs = append(errs, errors.Errorf(stringLengthErrorFormat, "consumer group name", nameMaxLength))
		}
	}

	if len(errs) > 0 {
		return &RequestValidationError{errs}
	}

	return nil
}

func (r *ConsumerGroupDeleteRequest) Validate() error {
	var errs []error

	if r.ConsumerGroup.Namespace == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "namespace"))
	} else if !nameRegex.MatchString(r.ConsumerGroup.Namespace) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "namespace"))
	} else if reservedNameRegex.MatchString(r.ConsumerGroup.Namespace) {
		errs = append(errs, errors.Errorf(reservedNameErrorFormat, "namespace"))
	} else if len(r.ConsumerGroup.Namespace) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "namespace", nameMaxLength))
	}

	if r.ConsumerGroup.Name == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "consumer group name"))
	} else if !nameRegex.MatchString(r.ConsumerGroup.Name) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "consumer group name"))
	} else if reservedNameRegex.MatchString(r.ConsumerGroup.Name) {
		errs = append(errs, errors.Errorf(reservedNameErrorFormat, "consumer group name"))
	} else if len(r.ConsumerGroup.Name) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "consumer group name", nameMaxLength))
	}

	if len(errs) > 0 {
		return &RequestValidationError{errs}
	}

	return nil
}

func (r *TopicPublishRequest) Validate() error {
	var errs []error

	if r.Topic.Namespace == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "namespace"))
	} else if !nameRegex.MatchString(r.Topic.Namespace) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "namespace"))
	} else if reservedNameRegex.MatchString(r.Topic.Namespace) {
		errs = append(errs, errors.Errorf(reservedNameErrorFormat, "namespace"))
	} else if len(r.Topic.Namespace) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "namespace", nameMaxLength))
	}

	if r.Topic.Name == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "topic name"))
	} else if !nameRegex.MatchString(r.Topic.Name) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "topic name"))
	} else if reservedNameRegex.MatchString(r.Topic.Name) {
		errs = append(errs, errors.Errorf(reservedNameErrorFormat, "topic name"))
	} else if len(r.Topic.Name) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "topic name", nameMaxLength))
	}

	if len(errs) > 0 {
		return &RequestValidationError{errs}
	}

	return nil
}

func (r *ConsumerGroupSubscribeRequest) Validate() error {
	var errs []error

	if r.ConsumerGroup.Namespace == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "namespace"))
	} else if !nameRegex.MatchString(r.ConsumerGroup.Namespace) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "namespace"))
	} else if reservedNameRegex.MatchString(r.ConsumerGroup.Namespace) {
		errs = append(errs, errors.Errorf(reservedNameErrorFormat, "namespace"))
	} else if len(r.ConsumerGroup.Namespace) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "namespace", nameMaxLength))
	}

	if r.ConsumerGroup.Name == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "consumer group name"))
	} else if !nameRegex.MatchString(r.ConsumerGroup.Name) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "consumer group name"))
	} else if reservedNameRegex.MatchString(r.ConsumerGroup.Name) {
		errs = append(errs, errors.Errorf(reservedNameErrorFormat, "consumer group name"))
	} else if len(r.ConsumerGroup.Name) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "consumer group name", nameMaxLength))
	}

	if len(errs) > 0 {
		return &RequestValidationError{errs}
	}

	return nil
}
