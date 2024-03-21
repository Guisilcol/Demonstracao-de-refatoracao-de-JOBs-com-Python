import typing as T

def get_args(default_args: dict):
    """
    Get the arguments from the command line and return them as a dictionary.
    """
    
    from sys import argv
    
    def get_arguments_with_double_dash(args: T.List[T.AnyStr]):
        iterator = iter(args)
        for arg in iterator:
            if arg.startswith('--'):
                try:
                    # Fetch the next argument which is the value for the current key
                    value = next(iterator)
                    yield arg[2:], value
                except StopIteration:
                    pass
                
    args = dict(list(get_arguments_with_double_dash(argv)))
    
    for key, value in default_args.items():
        if key not in args:
            args[key] = value
            
    return args
