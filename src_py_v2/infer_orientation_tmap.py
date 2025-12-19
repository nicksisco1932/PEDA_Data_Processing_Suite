import numpy as np
from scipy.io import loadmat
from pathlib import Path

# --- EDIT THESE PATHS ---
mat_path = Path(r"D:\093_01-098 PEDAv9.1.3-Data\093_01-098 PEDAv9.1.3-Data\Segment 1\TMap.mat")
npy_path = Path(r"D:\093_01-098\TDC_093-01_098\output\093_01-098 TDC Sessions\2025-11-05--07-05-25\PEDA\TMap.npy")

# --- LOAD DATA ---
mat_data = loadmat(mat_path)
TMap_mat = mat_data["TMap"]          # MATLAB 4D: (x,y,slice,time) or similar
TMap_py = np.load(npy_path)         # Python 4D: (x,y,slice,time) or whatever you wrote

# Choose one slice + last frame that you know matches the MATLAB figure
slice_idx = 5      # MATLAB slice 6 → zero-based index 5
frame_idx = -1     # last frame

A = np.squeeze(TMap_mat[:, :, slice_idx, frame_idx]).astype(float)
B = np.squeeze(TMap_py[:, :, slice_idx, frame_idx]).astype(float)

# Normalize for comparison
A = (A - np.nanmean(A)) / (np.nanstd(A) + 1e-6)
B = (B - np.nanmean(B)) / (np.nanstd(B) + 1e-6)

def candidates(arr):
    # All 8 basic 2D variants
    yield "identity", arr
    yield "flipud", np.flipud(arr)
    yield "fliplr", np.fliplr(arr)
    yield "flipud+fliplr", np.fliplr(np.flipud(arr))
    t = arr.T
    yield "transpose", t
    yield "transpose+flipud", np.flipud(t)
    yield "transpose+fliplr", np.fliplr(t)
    yield "transpose+flipud+fliplr", np.fliplr(np.flipud(t))

def score(A, Bcand):
    if A.shape != Bcand.shape:
        return np.inf
    diff = A - Bcand
    return np.nanmean(np.abs(diff))

best_name, best_err = None, np.inf
for name, Bc in candidates(B):
    err = score(A, Bc)
    print(f"{name:25s}  mean|diff| = {err:.4f}")
    if err < best_err:
        best_err = err
        best_name = name

print("\nBEST ORIENTATION:", best_name, "with mean|diff| =", best_err)
