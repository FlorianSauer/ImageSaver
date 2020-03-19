from typing import Optional

from tqdm import tqdm


class TqdmUpTo(tqdm):
    """Provides `update_to(n)` which uses `tqdm.update(delta_n)`."""

    def __init__(self, iterable=None, desc=None, total=None, leave=True, file=None, ncols=None, mininterval=0.1,
                 maxinterval=10.0, miniters=None, ascii=None, disable=False, unit='it', unit_scale=False,
                 dynamic_ncols=False, smoothing=0.3, bar_format=None, initial=0, position=None, postfix=None,
                 unit_divisor=1000, gui=False, **kwargs):
        super().__init__(iterable, desc, total, leave, file, ncols, mininterval, maxinterval, miniters, ascii, disable,
                         unit, unit_scale, dynamic_ncols, smoothing, bar_format, initial, position, postfix,
                         unit_divisor, gui, **kwargs)

    # def __init__(self, iterable=None, desc=None, total=None, leave=True, file=None, ncols=None, mininterval=0.1,
    #              maxinterval=10.0, miniters=None, ascii=None, disable=False, unit='it', unit_scale=False,
    #              dynamic_ncols=False, smoothing=0.3, bar_format=None, initial=0, position=None, postfix=None,
    #              unit_divisor=1000, gui=False, **kwargs):
    #     super().__init__(iterable, desc, total, leave, file, ncols, mininterval, maxinterval, miniters, ascii, disable,
    #                      unit, unit_scale, dynamic_ncols, smoothing, bar_format, initial, position, postfix,
    #                      unit_divisor, gui, **kwargs)
    #     self._last = 0
    #
    # def update_add(self, transferred, total):


    def update_to(self, b=1, bsize=1, tsize=None):
        # type: (Optional[int], Optional[int], Optional[int]) -> None
        """
        b  : int, optional
            Number of blocks transferred so far [default: 1].
        bsize  : int, optional
            Size of each block (in tqdm units) [default: 1].
        tsize  : int, optional
            Total size (in tqdm units). If [default: None] remains unchanged.
        """
        if tsize is not None:
            self.total = tsize
        chunk = b * bsize
        if tsize:
            chunk = tsize if chunk > tsize else chunk
        self.update(chunk - self.n)  # will also set self.n = b * bsize
