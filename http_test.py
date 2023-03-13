from unittest.mock import Mock

import main


def test_print_name():
    req = Mock(get_json=Mock(
        {'trigger' : 'http'}
    ))

    # Call tested function
    assert main.be_main(req) == 'ok'
