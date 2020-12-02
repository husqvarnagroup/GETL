from typeguard import check_type


def is_type(value, data_type) -> bool:
    try:
        check_type("type_check", value, data_type)
        return True
    except TypeError:
        return False
