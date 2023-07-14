class Person(object):
    number = 0  # <= define a static variable

    def __init__(self, name):
        self.name = name

    def say_hello(self):
        print("Hello, I'm {name}!".format(name = self.name))

    def say_number(self):
        print self.number
        self.number += 1

print '----- check number in Person'
print Person.number  # return 0
Person.number += 1
print Person.number  # return 1
print ''

print '----- check number in John'
john = Person('John')
john.say_hello()
john.say_number()  # return 1
john.say_number()  # return 2

print '----- check number in Mary'
mary = Person('Mary')
mary.say_hello()
mary.say_number()  # return 1
mary.say_number()  # return 2
