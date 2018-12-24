package authentication

type Provider interface {
	Mechanism() string
	Authenticate(challenge string, response string) (token Token, nextChallenge string, err error)
}

type Token interface {
	Subject() string
	IsAuthenticated() bool
}

type UsernamePasswordToken struct {
	Username string
	Password string
}

func (t *UsernamePasswordToken) Subject() string {
	return t.Username
}

func (t *UsernamePasswordToken) IsAuthenticated() bool {
	return true
}

type UsernamePasswordVerifier func(username, password string) (bool, error)

type NotAuthenticatedToken struct {
	Username string
}

func (t *NotAuthenticatedToken) Subject() string {
	return t.Username
}

func (t *NotAuthenticatedToken) IsAuthenticated() bool {
	return false
}
