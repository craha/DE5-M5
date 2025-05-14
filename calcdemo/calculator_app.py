# Demo of class
import pandas as pd

class Calculator:
    # Constructor (initialises variables)
    def __init__(self, a, b):
        self.a = a
        self.b = b
    
    # A method is a funciton that operates on an object
    def do_sum(self):
        return self.a + self.b
    
    def do_product(self):
        return self.a * self.b
    
    def do_subtract(self):
        return self.a - self.b
    
    def do_divide(self):
        return round((self.a / self.b), 2)


# Instantiation: turn this into main and take args with argparser or ENV variables
myCalc = Calculator(384, 97)
print(f"Answer is: {myCalc.do_product()}")

rows = []
for x in range(1, 11):
    calc3x = Calculator(3,x)
    rows.append({'factor': x, 'product': calc3x.do_product()}
                )

table3x = pd.DataFrame(rows)
print(table3x)