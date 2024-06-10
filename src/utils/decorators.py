import time


############################################################################################
###### DECORATOR FOR TIMING FUNCTIONS
############################################################################################
def time_counter(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print(f"Function {func.__name__} took {end - start} seconds to run")

        return result

    return wrapper
