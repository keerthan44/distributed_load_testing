class VariableListener:
    def __init__(self, value, callback):
        self._my_variable = value
        self._callback = callback

    @property
    def my_variable(self):
        return self._my_variable

    @my_variable.setter
    def my_variable(self, value):
        # Perform actions when the variable is changed
        print("Changed")
        self._my_variable = value
        if len(self._my_variable.active_count) == 0:
            self._callback()
        
