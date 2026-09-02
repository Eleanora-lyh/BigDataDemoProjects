# ADTK Python 项目搭建与验证指南

本文只记录已经验证成功的项目搭建流程。每一步都包含具体操作、这样做的原因和成功判据，可以直接用于在 Windows + VS Code 环境中从零复现本项目。

> 验证日期：2026-09-01  
> 操作系统：Windows  
> 仓库根目录：`BigDataLearningNotes`  
> Demo 项目根目录：`BigDataLearningNotes\ADTK\Demo`

除步骤 1 外，下面的 PowerShell 命令均在 `ADTK\Demo` 项目根目录中执行。

## 1. 最终项目状态

项目最终使用独立的 Python 虚拟环境运行 ADTK 示例：

| 项目     | 最终值                             | 作用                              |
| ------ | ------------------------------- | ------------------------------- |
| 环境类型   | `venv`                          | 隔离本项目与系统 Python 的依赖             |
| Python | `3.12.10`                       | 兼顾 Python 3.12 支持和 ADTK 旧依赖的兼容性 |
| 解释器    | `.venv\Scripts\python.exe`      | 安装依赖、运行脚本和 VS Code 分析统一使用该解释器   |
| ADTK   | `0.6.2`                         | 当前 PyPI 上使用的 ADTK 版本            |
| 示例入口   | `examples\basic_detection.py`   | 生成合成时序、检测异常并绘图                  |
| 图片输出   | `artifacts\basic_detection.png` | 保存检测结果，便于人工检查                   |

最终核心依赖版本如下：

| 包              | 已验证版本    |
| -------------- | -------- |
| `adtk`         | `0.6.2`  |
| `numpy`        | `1.26.4` |
| `pandas`       | `2.3.3`  |
| `scikit-learn` | `1.5.2`  |
| `statsmodels`  | `0.14.6` |
| `matplotlib`   | `3.10.9` |

## 2. 从零搭建与验证

### 步骤 1：创建并进入独立 Demo 目录

在 VS Code 中打开 `BigDataLearningNotes` 文件夹，然后从仓库根目录创建并进入 ADTK Demo：

```powershell
New-Item -ItemType Directory -Force .\ADTK\Demo | Out-Null
Set-Location .\ADTK\Demo
Get-Location
py -3.12 --version
```

`Get-Location` 应以 `BigDataLearningNotes\ADTK\Demo` 结尾，版本命令应输出 `Python 3.12.x`。当前项目验证时使用的是 `Python 3.12.10`。

使用独立 Demo 目录的原因：

- ADTK 的源码、依赖、虚拟环境和生成物集中在同一目录中。
- 仓库以后加入其他框架或示例时，可以为每个项目保留独立环境。
- 从 Demo 目录运行后，后续命令仍可使用简短的 `.venv`、`examples` 和 `artifacts` 相对路径。

明确指定 Python 3.12，是为了避免误用本机的 Python 3.13，并让虚拟环境与本文验证过的版本保持一致。

最终目录结构：

```text
BigDataLearningNotes/
└── ADTK/
    └── Demo/
        ├── .venv/                         # Demo 专用虚拟环境，不提交到 Git
        ├── .vscode/
        │   └── settings.json              # 单独打开 Demo 时的 VS Code 设置
        ├── artifacts/                     # 示例生成的图片，不提交到 Git
        ├── assets/                        # 文档使用的结果截图
        ├── examples/
        │   └── basic_detection.py         # ADTK 基础检测示例
        ├── .gitignore
        ├── ADTK_PROJECT_SETUP.md          # 本文档
        ├── README.md                      # Demo 使用入口
        └── requirements.txt               # 可复现依赖约束
```

成功判据：`Get-Location` 指向 `ADTK\Demo`，并且 `py -3.12 --version` 能正常输出版本。

### 步骤 2：创建并确认虚拟环境

在 `ADTK\Demo` 中创建该 Demo 专用的 `.venv`：

```powershell
py -3.12 -m venv .venv
```

虚拟环境创建后，直接使用其中的解释器确认版本：

```powershell
.\.venv\Scripts\python.exe --version
```

该解释器的路径为：

```text
<仓库根目录>\ADTK\Demo\.venv\Scripts\python.exe
```

本项目确认的 Python 版本为 `3.12.10`。

这样做的原因：

- ADTK 0.6.2 发布较早，先在独立环境中验证不会污染系统 Python。
- 机器上还存在 Python 3.13，最初编辑器可能选中系统解释器；项目实际需要统一使用 `.venv`。
- 依赖安装、编辑器类型分析和脚本运行使用同一解释器，可以避免“终端能运行但编辑器报无法导入”或相反的情况。

后续命令都显式调用 `.\.venv\Scripts\python.exe`，不依赖终端中的 `python` 或虚拟环境激活状态。这样即使系统 `PATH` 指向其他 Python，也不会安装到错误的环境。

在 VS Code 中确认解释器的方法：

1. 按 `Ctrl+Shift+P`。
2. 执行 `Python: Select Interpreter`。
3. 如果单独打开了 `ADTK\Demo`，选择 `.venv\Scripts\python.exe`。
4. 如果打开的是整个仓库，选择 `ADTK\Demo\.venv\Scripts\python.exe`。
5. 查看 VS Code 状态栏，确认当前解释器来自 Demo 的 `.venv`。

`ADTK\Demo\.vscode\settings.json` 也配置了 Demo 的默认解释器。该配置在单独用 VS Code 打开 `ADTK\Demo` 文件夹时生效。

成功判据：项目根目录中存在 `.venv`，版本检查成功，并且 VS Code 状态栏显示该虚拟环境。

### 步骤 3：编写兼容的依赖约束

创建 `requirements.txt`，最终内容为：

```text
adtk==0.6.2
matplotlib>=3.8,<3.11
numpy>=1.26,<2
pandas>=2.1,<3
scikit-learn>=1.3,<1.6
statsmodels>=0.14,<0.15
```

ADTK 0.6.2 发布于 2020 年，主要依赖没有完整的版本上限。仅执行不带约束的 `pip install adtk`，可能会得到远晚于 ADTK 发布的新主版本。`pip` 能完成安装并不代表这些包的运行时 API 仍与 ADTK 兼容，因此本项目主动限定经过验证的范围。

各项约束的作用：

| 约束                        | 原因                                                   |
| ------------------------- | ---------------------------------------------------- |
| `adtk==0.6.2`             | 固定学习目标，避免未来解析到行为不同的版本                                |
| `numpy>=1.26,<2`          | Python 3.12 需要较新的 NumPy，同时避开 NumPy 2 的大版本兼容风险        |
| `pandas>=2.1,<3`          | 保留 Python 3.12 支持，避开 pandas 3 的 API 变化               |
| `scikit-learn>=1.3,<1.6`  | 使用具有 Python 3.12 轮子的版本，并限制在已验证范围内                    |
| `statsmodels>=0.14,<0.15` | 使用支持当前 Python 的 0.14 系列                              |
| `matplotlib>=3.8,<3.11`   | Python 3.12 使用新版 Matplotlib，但避开 3.11 删除的 ADTK 依赖 API |

这里使用“版本范围”而不是把所有传递依赖完全锁死，是为了让学习项目在兼容区间内仍能接收补丁版本。上表中的具体版本则是本机已经验证通过的组合。

成功判据：项目根目录存在 `requirements.txt`，并且内容包含上述六项约束。

### 步骤 4：安装项目依赖

先升级虚拟环境中的 `pip`：

```powershell
.\.venv\Scripts\python.exe -m pip install --upgrade pip
```

再按照项目约束安装依赖：

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

使用 `.venv` 中的 `python.exe -m pip`，可以明确保证依赖安装到当前项目环境，而不是系统 Python。

可以用下面的命令查看核心包的实际版本：

```powershell
.\.venv\Scripts\python.exe -m pip show adtk matplotlib numpy pandas scikit-learn statsmodels
```

本项目安装并验证通过的核心组合为：

```text
ADTK 0.6.2
NumPy 1.26.4
pandas 2.3.3
scikit-learn 1.5.2
statsmodels 0.14.6
Matplotlib 3.10.9
```

成功判据：安装命令正常结束，`pip show` 能找到上述六个包，并且版本位于 `requirements.txt` 指定的范围内。

**检查依赖冲突**

```powershell
.\.venv\Scripts\python.exe -m pip check
```

结果：`No broken requirements found.`

作用：确认当前已安装包之间没有缺失依赖或声明冲突。

### 步骤 5：创建项目文件

创建或补充以下文件：

#### `.gitignore`

忽略以下本地文件和生成物：

```text
.venv/
__pycache__/
*.py[cod]
.pytest_cache/
.mypy_cache/
artifacts/
```

原因是虚拟环境体积较大且与机器相关，缓存和生成图片也可以由代码重新产生，不应提交到 Git。

#### `README.md`

提供项目简介、环境要求、安装命令、示例运行命令和后续 ADTK 学习方向。它作为日常使用入口，本文则保留更详细的搭建与排查过程。

#### `examples/basic_detection.py`

先创建示例目录：

```powershell
New-Item -ItemType Directory -Force examples | Out-Null
```

`examples\basic_detection.py` 的完整代码如下：

```python
from pathlib import Path

import matplotlib

# Select a non-interactive backend before importing pyplot so the example can
# save a chart without opening a desktop window.
matplotlib.use("Agg")

import numpy as np
import pandas as pd
from adtk.data import to_events, validate_series
from adtk.detector import InterQuartileRangeAD
from adtk.visualization import plot
from matplotlib import pyplot as plt


# ADTK 0.6.2 requests the old name for this bundled Matplotlib style.
if "seaborn-whitegrid" not in plt.style.library:
    plt.style.library["seaborn-whitegrid"] = plt.style.library[
        "seaborn-v0_8-whitegrid"
    ]


def build_sample_series() -> tuple[pd.Series, pd.DatetimeIndex]:
    """Create a regular hourly series with two known outliers."""
    # A fixed seed makes the generated data and detection result reproducible.
    random_generator = np.random.default_rng(seed=42)
    index = pd.date_range("2026-01-01", periods=24 * 14, freq="h")
    hours = np.arange(len(index))

    # Combine a daily cycle with small random noise to model a normal signal.
    values = (
        20
        + 2 * np.sin(2 * np.pi * hours / 24)
        + random_generator.normal(0, 0.35, len(index))
    )

    # Inject one high and one low outlier at known positions for verification.
    anomaly_positions = [24 * 4 + 6, 24 * 10 + 18]
    values[anomaly_positions] += [12, -12]

    series = pd.Series(values, index=index, name="metric")

    # ADTK expects an ordered DatetimeIndex with a valid, regular frequency.
    return validate_series(series), index[anomaly_positions]


def main() -> None:
    series, injected_anomalies = build_sample_series()

    # Fit IQR boundaries on the series and return one anomaly flag per point.
    detector = InterQuartileRangeAD(c=1.5)
    anomalies = detector.fit_detect(series)

    # Treat any undefined flags as normal before using the result as an index.
    detected_anomalies = anomalies.fillna(False).astype(bool)
    detected_times = series.index[detected_anomalies]

    # Fail loudly if the detector misses either deliberately injected outlier.
    missed_times = injected_anomalies.difference(detected_times)
    if not missed_times.empty:
        raise RuntimeError(f"Known anomalies were not detected: {list(missed_times)}")

    print(f"Detected {len(detected_times)} anomalous points:")
    for timestamp in detected_times:
        print(f"- {timestamp}: {series.loc[timestamp]:.2f}")

    # Convert point-wise flags into time intervals suitable for alert records.
    print(f"Anomaly events: {to_events(anomalies)}")

    # Keep generated files inside this demo, regardless of the terminal's CWD.
    project_root = Path(__file__).resolve().parents[1]
    output_path = project_root / "artifacts" / "basic_detection.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plot(
        series,
        anomaly=anomalies,
        anomaly_color="red",
        anomaly_tag="marker",
    )
    plt.title("ADTK interquartile-range anomaly detection")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close("all")
    print(f"Chart saved to {output_path.resolve()}")


if __name__ == "__main__":
    main()
```

示例按以下顺序工作：

1. 用固定随机种子生成 14 天的小时级周期数据。
2. 手工注入两个已知异常点。
3. 用 `validate_series` 校验时间序列格式。
4. 用 `InterQuartileRangeAD(c=1.5)` 拟合并检测异常。
5. 检查两个已知异常是否都被发现；若漏检则主动抛出异常。
6. 用 `to_events` 把布尔异常序列转换为事件。
7. 用 ADTK 的 `plot` 绘图并保存到 `artifacts\basic_detection.png`。

选择合成数据的原因：

- 不依赖网络和外部数据文件，任何机器都能直接运行。
- 已知异常点的位置，可对检测结果做自动断言，而不是只凭肉眼判断。
- 固定随机种子后结果可重复，便于修改检测器参数并比较变化。

脚本使用 Matplotlib 的 `Agg` 后端，因为示例的目标是保存图片，不要求弹出桌面窗口。这也更适合自动化验证。

ADTK 0.6.2 使用旧 Matplotlib 样式名 `seaborn-whitegrid`，当前示例在旧名称不存在时，把它映射到新版名称 `seaborn-v0_8-whitegrid`。这个兼容处理让代码仍然调用 ADTK 原生的 `plot`，同时能够使用当前验证过的 Matplotlib。

脚本通过 `Path(__file__).resolve().parents[1]` 从脚本位置定位 `ADTK\Demo`，因此即使从仓库根目录启动，图片也会写入本 Demo 的 `artifacts`。`output_path.parent.mkdir(parents=True, exist_ok=True)` 会自动创建输出目录，第一次运行前不需要手工创建。

成功判据：`.gitignore`、`README.md` 和 `examples\basic_detection.py` 均已存在，示例文件包含数据生成、检测、结果断言和绘图逻辑。

### 步骤 6：执行语法检查

正式运行前，先检查示例能否被 Python 正确编译：

```powershell
.\.venv\Scripts\python.exe -m py_compile examples\basic_detection.py
```

语法检查成本很低，可以先排除缩进、括号和其他语法错误，再把完整运行用于验证依赖和程序行为。命令没有输出且正常返回，就表示检查通过。

成功判据：命令退出码为 `0`，终端中没有错误信息。

### 步骤 7：端到端运行验证

执行：

```powershell
.\.venv\Scripts\python.exe examples\basic_detection.py
```

最终结果：

- ADTK 成功导入。
- 两个注入的异常点都被检测到。
- `to_events` 成功生成异常事件。
- 图片成功保存到 `artifacts\basic_detection.png`。
- 图片中的异常点以红色标记显示，位置与注入点一致。

运行输出为：

```text
(.venv) PS ProjectRoot\BigDataLearningNotes\ADTK\Demo> .\.venv\Scripts\python.exe examples\basic_detection.py
Detected 2 anomalous points:
- 2026-01-05 06:00:00: 33.88
- 2026-01-11 18:00:00: 5.95
Anomaly events: [(Timestamp('2026-01-05 06:00:00'), Timestamp('2026-01-05 06:59:59.999999999')), (Timestamp('2026-01-11 18:00:00'), Timestamp('2026-01-11 18:59:59.999999999'))]
Chart saved to ProjectRoot\BigDataLearningNotes\ADTK\Demo\artifacts\basic_detection.png
```

打开图片显示

![](assets/2026-09-01-17-20-17-image.png)

一次完整运行会同时验证 NumPy 和 pandas 数据构造、ADTK 导入、序列校验、检测器拟合、异常事件转换、Matplotlib 绘图以及文件写入路径。

成功判据：程序退出码为 `0`，终端列出两个异常时间，并且生成的图片中有两个红色异常标记。

## 3. 最终验证清单

完成以上步骤后，项目应满足：

- `.venv` 使用 Python 3.12，并与系统 Python 隔离。
- VS Code/Pylance 选择 `ADTK\Demo\.venv\Scripts\python.exe`。
- 核心依赖位于 `requirements.txt` 指定的兼容范围内。
- `basic_detection.py` 语法检查通过。
- 示例准确检测两个已知异常点。
- `artifacts\basic_detection.png` 成功生成。
- `pip check` 未发现依赖问题。
- `.venv`、缓存文件和生成图片不会被 Git 跟踪。

至此，ADTK 学习验证项目已经可以稳定运行。后续可以在同一个虚拟环境中继续练习 `ThresholdAD`、`PersistAD`、`SeasonalAD`，或把合成时间序列替换为自己的 CSV 数据。