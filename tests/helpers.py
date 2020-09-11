from random import shuffle

def create_dt_params(columns, search='', start=0, length=10, order=None):
    """Create DataTables input parameters when the data source from the rows
    data object/ array is not set. 
    
    Read more about setting column data source here https://datatables.net/reference/option/columns.data"""
    params = {
        'draw': '1',
        'start': str(start),
        'length': str(length),
        'search[value]': str(search),
        'search[regex]': 'false'
    }

    for i, item in enumerate(columns):
        cols = 'columns[%s]' % i
        params['%s%s' % (cols, '[data]')] = i
        params['%s%s' % (cols, '[name]')] = ''
        params['%s%s' % (cols, '[searchable]')] = 'true'
        params['%s%s' % (cols, '[orderable]')] = 'true'
        params['%s%s' % (cols, '[search][value]')] = ''
        params['%s%s' % (cols, '[search][regex]')] = 'false'

    for i, item in enumerate(order or [{'column': 0, 'dir': 'asc'}]):
        for key, value in item.items():
            params['order[%s][%s]' % (i, key)] = str(value)

    return params

# These methods would only be used when the mData param is defined in the backend

def create_dt_params_with_mData(columns, search='', start=0, length=10, order=None):
    """Create DataTables input parameters when the data source from the rows
    data object/ array is set. 
    
    Read more about setting column data source here https://datatables.net/reference/option/columns.data"""
    
    params = {
        'draw': '1',
        'start': str(start),
        'length': str(length),
        'search[value]': str(search),
        'search[regex]': 'false'
    }

    for i, item in enumerate(columns):
        cols = 'columns[%s]' % i
        params['%s%s' % (cols, '[data]')] = item.mData
        params['%s%s' % (cols, '[name]')] = ''
        params['%s%s' % (cols, '[searchable]')] = 'true'
        params['%s%s' % (cols, '[orderable]')] = 'true'
        params['%s%s' % (cols, '[search][value]')] = ''
        params['%s%s' % (cols, '[search][regex]')] = 'false'

    for i, item in enumerate(order or [{'column': 0, 'dir': 'asc'}]):
        for key, value in item.items():
            params['order[%s][%s]' % (i, key)] = str(value)

    return params

def create_dt_params_with_mData_shuffled(columns, search='', start=0, length=10, order=None):
    """Create DataTables input parameters when the data source from the rows
    data object/ array is set. Also when the order in the frontend is not same
    as in the backend.
    
    Read more about setting column data source here https://datatables.net/reference/option/columns.data"""
    
    params = {
        'draw': '1',
        'start': str(start),
        'length': str(length),
        'search[value]': str(search),
        'search[regex]': 'false'
    }
    # Shuffle the columns in place
    shuffle(columns) 
    for i, item in enumerate(columns):
        cols = 'columns[%s]' % i
        params['%s%s' % (cols, '[data]')] = item.mData
        params['%s%s' % (cols, '[name]')] = ''
        params['%s%s' % (cols, '[searchable]')] = 'true'
        params['%s%s' % (cols, '[orderable]')] = 'true'
        params['%s%s' % (cols, '[search][value]')] = ''
        params['%s%s' % (cols, '[search][regex]')] = 'false'

    for i, item in enumerate(order or [{'column': 0, 'dir': 'asc'}]):
        for key, value in item.items():
            params['order[%s][%s]' % (i, key)] = str(value)

    return params

def create_dt_params_with_mData_with_extra_data(columns, search='', start=0, length=10, order=None):
    """Create DataTables input parameters when the data source from the rows
    data object/ array is set. Also when there is an extra data source defined in
    the frontend just for the use in the frontend but not in the backend.
    An example of this is here https://editor.datatables.net/examples/bubble-editing/simple.html
    
    Read more about setting column data source here https://datatables.net/reference/option/columns.data"""
    
    params = {
        'draw': '1',
        'start': str(start),
        'length': str(length),
        'search[value]': str(search),
        'search[regex]': 'false'
    }
    # Add the extra params for the extra data source added in the frontend but
    # not in the backend.
    params['columns[0][name]'] = ''
    params['columns[0][searchable]'] = 'true'
    params['columns[0][orderable]'] = 'false'
    params['columns[0][search][value]'] = ''
    params['columns[0][search][regex]'] = 'false'
    for i, item in enumerate(columns, 1):
        cols = 'columns[%s]' % i
        params['%s%s' % (cols, '[data]')] = item.mData
        params['%s%s' % (cols, '[name]')] = ''
        params['%s%s' % (cols, '[searchable]')] = 'true'
        params['%s%s' % (cols, '[orderable]')] = 'true'
        params['%s%s' % (cols, '[search][value]')] = ''
        params['%s%s' % (cols, '[search][regex]')] = 'false'

    for i, item in enumerate(order or [{'column': 1, 'dir': 'asc'}]):
        for key, value in item.items():
            params['order[%s][%s]' % (i, key)] = str(value)

    return params