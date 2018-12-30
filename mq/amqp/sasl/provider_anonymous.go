package sasl

type anonymousProvider struct {
}

func NewANONYMOUS() Provider {
	return &anonymousProvider{}
}

func (*anonymousProvider) Mechanism() string {
	return "ANONYMOUS"
}

func (*anonymousProvider) Authenticate(challenge string, response string) (token Token, nextChallenge string, err error) {
	return &AnonymousToken{}, "", nil
}
