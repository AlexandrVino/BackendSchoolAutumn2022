def validate_folder(kwargs):
    """
    :param kwargs: словарь
    :return: None

    Функция проверки категорий на валидность
    """

    keys = [
        ('uid', (1,)), ('date', (1,)), ('parentId', (0, 1)), ('type', (1,)), ('size', (0,))
    ]
    for key, value in keys:
        assert bool(kwargs.get(key)) in value, 'Validation failed'


def validate_file(kwargs):
    """
    :param kwargs: словарь
    :return: None

    Функция проверки товаров на валидность
    """

    keys = [
        ('uid', (1,)), ('url', (1,)), ('date', (1,)), ('parentId', (0, 1)), ('type', (1,)), ('size', (1,))
    ]
    for key, value in keys:
        if key == 'price':
            assert kwargs.get(key) >= 0, 'Validation failed'
        assert bool(kwargs.get(key)) in value, 'Validation failed'


def validate_all_items(items: iter):
    """
    :param items: iter-объект
    :return: None

    Функция проверки категорий и товаров на валидность
    """

    funcs = {
        'folder': validate_folder,
        'file': validate_file,
    }

    for item in items:
        funcs[item[0]['type']](item[0])
