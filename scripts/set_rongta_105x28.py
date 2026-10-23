# Add Windows paper form 105x28 mm and set it as Rongta default.
import ctypes
from ctypes import wintypes

PRINTER = "Rongta RP4xx Series"
FORM = "Badail 105x28"
# FORM_INFO_1 Size is thousandths of a millimeter
W_UM = 105000
H_UM = 28000

winspool = ctypes.WinDLL("winspool.drv")


class SIZEL(ctypes.Structure):
    _fields_ = [("cx", ctypes.c_long), ("cy", ctypes.c_long)]


class RECTL(ctypes.Structure):
    _fields_ = [
        ("left", ctypes.c_long),
        ("top", ctypes.c_long),
        ("right", ctypes.c_long),
        ("bottom", ctypes.c_long),
    ]


class FORM_INFO_1(ctypes.Structure):
    _fields_ = [
        ("Flags", wintypes.DWORD),
        ("pName", wintypes.LPWSTR),
        ("Size", SIZEL),
        ("ImageableArea", RECTL),
    ]


class PRINTER_DEFAULTS(ctypes.Structure):
    _fields_ = [
        ("pDatatype", wintypes.LPWSTR),
        ("pDevMode", ctypes.c_void_p),
        ("DesiredAccess", wintypes.DWORD),
    ]


PRINTER_ACCESS_USE = 0x00000008
PRINTER_ACCESS_ADMINISTER = 0x00000004
STANDARD_RIGHTS_REQUIRED = 0x000F0000
PRINTER_ALL_ACCESS = STANDARD_RIGHTS_REQUIRED | PRINTER_ACCESS_ADMINISTER | PRINTER_ACCESS_USE

OpenPrinterW = winspool.OpenPrinterW
OpenPrinterW.argtypes = [wintypes.LPWSTR, ctypes.POINTER(wintypes.HANDLE), ctypes.c_void_p]
OpenPrinterW.restype = wintypes.BOOL
ClosePrinter = winspool.ClosePrinter
ClosePrinter.argtypes = [wintypes.HANDLE]
ClosePrinter.restype = wintypes.BOOL
AddFormW = winspool.AddFormW
AddFormW.argtypes = [wintypes.HANDLE, wintypes.DWORD, ctypes.c_void_p]
AddFormW.restype = wintypes.BOOL
DeleteFormW = winspool.DeleteFormW
DeleteFormW.argtypes = [wintypes.HANDLE, wintypes.LPWSTR]
DeleteFormW.restype = wintypes.BOOL


def open_printer(access):
    h = wintypes.HANDLE()
    defaults = PRINTER_DEFAULTS(None, None, access)
    if not OpenPrinterW(PRINTER, ctypes.byref(h), ctypes.byref(defaults)):
        err = ctypes.GetLastError()
        if not OpenPrinterW(PRINTER, ctypes.byref(h), None):
            raise OSError(err or ctypes.GetLastError(), "OpenPrinter failed")
    return h


def main():
    form = FORM_INFO_1()
    form.Flags = 0
    form.pName = FORM
    form.Size = SIZEL(W_UM, H_UM)
    form.ImageableArea = RECTL(0, 0, W_UM, H_UM)

    h = open_printer(PRINTER_ALL_ACCESS)
    try:
        DeleteFormW(h, FORM)
        if not AddFormW(h, 1, ctypes.byref(form)):
            raise OSError(ctypes.GetLastError(), "AddForm failed")
        print("OK form", FORM, "105mm x 28mm")
    finally:
        ClosePrinter(h)


if __name__ == "__main__":
    main()
