"""db_client_util"""

import re


def get_conditional(qry_str: str, params: dict) -> str:
    """
    return true or false part by condition.

    ex:
    SELECT  *
    FROM
    #if target == 'en'
            tbl_en
    #else
            tbl_ko
    #endif
    ->
    SELECT  *
    FROM
            tbl_en
    """

    def eval_safe(condition: str, params: dict) -> bool:
        """
        allow only params and operators for condition to call eval safely

        # assert eval_safe('%(target)s == "A"', {"target": "A"}) is True
        # assert eval_safe('%(target)s != "A"', {"target": "A"}) is False
        # assert eval_safe("%(target)s == 123", {"target": 123}) is True
        # assert eval_safe("%(target)s != 123", {"target": 123}) is False
        # assert eval_safe("%(target123)s == 123", {"target123": 123}) is True
        # assert eval_safe("%(target123)s != 123", {"target123": 123}) is False
        # assert eval_safe('%(target)s == ""', {"target": ""}) is True
        # assert eval_safe('%(target)s != ""', {"target": ""}) is False
        # assert eval_safe('"A" in %(targets)s', {"targets": ["A", "B"]}) is True
        # assert eval_safe('"A" not in %(targets)s', {"targets": ["A", "B"]}) is False
        # assert eval_safe("%(t)s in [i for i in range(10)]", {"t": 1}) is True  # '[i' not in
        """

        # remove '%(' and ')s' from %(target)s
        to_eval = re.sub(r"%\((\w+)\)s", r"\1", condition)

        # remove
        # - double quoted string
        # - single quoted string
        # - digit
        to_check = re.sub(r"""(".*?"|'.*?'|\b\d+\b)""", "", to_eval)

        param_set = set([key for key in params])
        op_set = {
            "==",
            "!=",
            ">=",
            ">",
            "<=",
            "<",
            "+",
            "-",
            "/",
            "//",
            "*",
            "**",
            "and",
            "or",
            "not",
            "in",
            "True",
            "False",
        }
        allowed_set = param_set | op_set

        for word in to_check.split():
            if word not in allowed_set:
                raise ValueError(f"'{word}' not in {allowed_set}")

        # pylint:disable=eval-used
        return eval(to_eval, params)

    rets = []
    lines = qry_str.splitlines()

    is_include = True
    is_checked = False
    for line in lines:
        line_strip = line.strip()
        if line_strip.startswith("#if") or line_strip.startswith("#elif"):
            if not is_checked:
                _, condition = line_strip.split(maxsplit=1)
                # pylint:disable=eval-used
                is_include = eval_safe(condition, params.copy())
                if is_include:
                    is_checked = True
            else:
                is_include = False
        elif line_strip.startswith("#else"):
            is_include = not is_checked
        elif line_strip.startswith("#endif"):
            is_include = True
            is_checked = False
        elif is_include:
            rets.append(line)

    return "\n".join(rets)


def rep_kv(query: str, tab_count: int, **kwargs) -> str:
    """
    replace {key} with value when `rev_ky("WHERE name = {key}", key="u.name")`
    """

    ret = query
    ret = re.sub(r"^", " " * 4 * tab_count, ret, flags=re.MULTILINE)
    for k, v in kwargs.items():
        ret = ret.replace("{" + k + "}", str(v))

    return ret
