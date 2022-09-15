
def acceptance_rate(y, selection):
    return sum(selection == 1) / len(selection)


def tpr(y, selection):
    return sum((y == 1) & (selection == 1)) / sum(y == 1)


def fpr(y, selection):
    return sum((y == 0) & (selection == 1)) / sum(y == 0)


def fnr(y, selection):
    return sum((y == 0) & (selection == 1)) / sum(y == 1)


def tnr(y, selection):
    return sum((y == 0) & (selection == 0)) / sum(y == 0)


def ppv(y, selection):
    if sum(selection == 1) == 0:
        return float('inf')
    return sum((y == 1) & (selection == 1)) / sum(selection == 1)


def fdr(y, selection):
    if sum(selection == 1) == 0:
        return float('inf')
    return sum((y == 0) & (selection == 1)) / sum(selection == 1)


def forate(y, selection):
    if sum(selection == 0) == 0:
        return float('inf')
    return sum((y == 1) & (selection == 0)) / sum(selection == 0)


def npv(y, selection):
    if sum(selection == 0) == 0:
        return float('inf')
    return sum((y == 0) & (selection == 0)) / sum(selection == 0)


all_metrics = {
    "acceptance_rate": acceptance_rate,
    "recall": tpr,
    "fpr": fpr,
    "fnr": fnr,
    "tnr": tnr,
    "precision": ppv,
    "fdr": fdr,
    "forate": forate,
    "npv": npv
}
