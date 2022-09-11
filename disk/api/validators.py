def validate_folder(kwargs):
    """
    :param kwargs: словарь
    :return: None

    Функция проверки категорий на валидность
    """

    keys = [('uid', (1,)), ('date', (1,)), ('parentId', (0, 1)), ('type', (1,)), ('size', (0,))]
    for key, value in keys:
        assert bool(kwargs.get(key)) in value, f'Validation failed, {key} must be set'


def validate_file(kwargs):
    """
    :param kwargs: словарь
    :return: None

    Функция проверки товаров на валидность
    """

    keys = [
        ('uid', (1,)), ('url', (1,)), ('date', (1,)), ('parentId', (0, 1)), ('type', ('folder', 'file')), ('size', (1,))
    ]

    for key, value in keys:
        if key == 'size':
            assert kwargs.get(key) > 0, 'Validation failed, size must be positive'
        elif key == 'type':
            assert kwargs.get(key) in value, f"Awaited type in ('folder', 'file'), got {kwargs.get(key)}"
        else:
            assert bool(kwargs.get(key)) in value, f'Validation failed, {key} must be set'


def validate_all_items(chunk: iter):
    """
    :param chunk: iter-объект
    :return: None

    Функция проверки категорий и товаров на валидность
    """

    funcs = {'folder': validate_folder, 'file': validate_file}

    for item_group in chunk:
        for item in item_group:
            funcs[item['type']](item)
