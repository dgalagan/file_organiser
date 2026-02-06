import keyboard

def test_press_esc():
    press_result = False
    
    while True:
        if keyboard.is_pressed('Esc'):
            press_result = True
            break
    
    assert press_result == True, "Does not react to Esc button"

if __name__ == "__main__":
    test_press_esc()