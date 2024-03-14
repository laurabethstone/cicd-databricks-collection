1. Schema Validation Decorator
def validate_schema(expected_schema):
    def decorator(func):
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            actual_schema = result.schema
            if expected_schema != actual_schema:
                raise ValueError(f"Schema mismatch. Expected: {expected_schema}, Actual: {actual_schema}")
            return result
        return wrapper
    return decorator

# usage 
@validate_schema(expected_schema)
def my_spark_function(*args, **kwargs):
    # Your Spark processing logic here
    return result

    
