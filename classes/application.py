class Application:


### Constructor from application class


    def __init__(self):
        self.appid = 0
        self.name = ''
        self.type = 'BATCH'
        self.image = ''
        self.min_memory = -1
        self.num_cores = 1
        self.comments = ''


### Function to verify an equality of two applications


    def __eq__(self, other):
        if isinstance(other, self.__class__):

            if self.appid == other.appid:
                return True

            else:
                return False


### Functions to print applications


    def __str__(self):
        out = []

        for key in self.__dict__:
            out.append("{key}='{value}'".format(key=key, value=self.__dict__[key]))

        return ', '.join(out)


    def __repr__(self):
        return self.name
