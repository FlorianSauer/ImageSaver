from typing import Optional

from tqdm import tqdm


class TqdmUpTo(tqdm):
    """Provides `update_to(n)` which uses `tqdm.update(delta_n)`."""

    def write(self, s, file=None, end="\n", nolock=False):
        if self.disable:
            return
        return super(TqdmUpTo, self).write(s, file, end, nolock)

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
