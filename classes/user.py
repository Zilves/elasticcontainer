class User:


### Constructor for user class


    def __init__(self):
        self.userid = -1
        self.login = ''
        self.password = ''
        self.name = ''
        self.type = 'USER'


### Function to verify an equality of two users


    def __eq__(self, other):
        if isinstance(other, self.__class__):

            if self.userid == other.userid:
                return True

            else:
                return False


### Functions to print users


    def __str__(self):
        out = []

        for key in self.__dict__:
            out.append("{key}='{value}'".format(key=key, value=self.__dict__[key]))

        return ', '.join(out)


    def __repr__(self):
        return self.login
