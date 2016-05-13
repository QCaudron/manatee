"""

.. moduleauthor:: Quentin Caudron <quentincaudron@gmail.com>

"""

from .manatee import Manatee
from setup import opts
 
__version__ = opts["version"]
__all__ = ["manatee", "utils"]

