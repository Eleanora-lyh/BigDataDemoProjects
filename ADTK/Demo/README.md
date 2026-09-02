# ADTK 学习验证项目

这是一个用于练习 [ADTK](https://adtk.readthedocs.io/) 时间序列异常检测的最小 Python 项目。示例不依赖外部数据，会生成带有两个已知异常点的小时级时间序列，然后完成数据校验、模型拟合、异常检测和结果绘图。

项目位于 `BigDataLearningNotes\ADTK\Demo`，拥有独立的源码、依赖和虚拟环境，不会与仓库中的其他学习项目混用配置。

## 环境

- Windows
- Python 3.12
- ADTK 0.6.2
- 项目虚拟环境：`.venv`

ADTK 0.6.2 发布于 2020 年，依赖声明没有上限。`requirements.txt` 因此限制了主要科学计算包的版本范围，避免直接解析到未经 ADTK 验证的新主版本。

## 进入项目目录

从 `BigDataLearningNotes` 仓库根目录执行：

```powershell
Set-Location .\ADTK\Demo
```

下文命令均在 `ADTK\Demo` 中运行。

## 安装

在 VS Code 终端中运行：

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

在 VS Code 中选择解释器时，使用 `.venv\Scripts\python.exe`。

完整的环境搭建过程和代码讲解见 [ADTK_PROJECT_SETUP.md](ADTK_PROJECT_SETUP.md)。

## 运行第一个示例

```powershell
.\.venv\Scripts\python.exe examples\basic_detection.py
```

成功运行后，终端会列出检测到的异常时间，并生成 `artifacts/basic_detection.png`。

示例覆盖四个核心步骤：

1. 使用带固定频率的 `DatetimeIndex` 创建 `pandas.Series`。
2. 使用 `adtk.data.validate_series` 校验并规范化时序数据。
3. 使用 `InterQuartileRangeAD.fit_detect` 拟合并检测异常。
4. 使用 `to_events` 和 `plot` 转换、展示检测结果。

## 后续练习

- 修改 `InterQuartileRangeAD(c=1.5)` 的 `c`，观察灵敏度变化。
- 用 `ThresholdAD` 验证已知上下限规则。
- 用 `PersistAD` 检测相邻时间点之间的突变。
- 用 `SeasonalAD(freq=24)` 检测小时数据中的日周期异常。
- 将合成数据替换为 CSV；读取后把时间列转换成 `DatetimeIndex`，再调用 `validate_series`。