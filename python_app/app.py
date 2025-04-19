# Kivy App Import
from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
#------------------------------------------------------------------------------
# Function App Core import
from BLUETOOTH_FUNC.appEndFunction import CHECK_BLUETOOTH
#------------------------------------------------------------------------------

# SRC_APP_CODE_BELOW

class MyAppWiget(BoxLayout):
    def __init__(self, **kwargs):
        super(MyAppWiget, self).__init__(**kwargs)
         
    def CHECK_BLUETOOTH_CONNECTION_BUTTON(self):
        check_bluetooth = CHECK_BLUETOOTH()
        self.status_content.text = check_bluetooth.run()

class MyApp(App):

    def build(self):
        return MyAppWiget()
    
if __name__ == '__main__':
    MyApp().run()