from pathlib import Path

import numpy as np

from monetio import hytraj

HERE = Path(__file__).parent


def test_001():
    """
    test for combine_dataset function
    """
    tdump1 = HERE / "data/tdump.1"
    tdump2 = HERE / "data/tdump.2"
    flist = [tdump1, tdump2]
    taglist = ["t1", "t2"]

    # don't renumber
    dff = hytraj.combine_dataset(flist, taglist, renumber=False)
    alist = [1, 2, 1, 2, 1, 2, 1, 2, 3, 1, 2, 3, 1, 2, 3]
    tlist = [taglist[0]] * 6
    tlist.extend([taglist[1]] * 9)
    for iii in np.arange(0, 15):
        assert dff["traj_num"].values[iii] == alist[iii]
        assert dff["pid"].values[iii] == tlist[iii]

    # renumber but don't tag
    alist = [1, 2, 1, 2, 1, 2, 3, 4, 5, 3, 4, 5, 3, 4, 5]
    dff = hytraj.combine_dataset(flist, taglist=None, renumber=True)
    for iii in np.arange(0, 15):
        assert dff["traj_num"].values[iii] == alist[iii]

    # don't renumber and need to generate taglist internally
    alist = [1, 2, 1, 2, 1, 2, 1, 2, 3, 1, 2, 3, 1, 2, 3]
    tlist = [1] * 6
    tlist.extend([2] * 9)
    dff = hytraj.combine_dataset(flist, taglist=None, renumber=None)
    for iii in np.arange(0, 15):
        assert dff["traj_num"].values[iii] == alist[iii]
        assert dff["pid"].values[iii] == tlist[iii]
    print(dff)


test_001()
