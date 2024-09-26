import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
from sklearn.utils import class_weight
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, LSTM

# 读取数据
data = pd.read_csv('stockDataSet/2301.csv', parse_dates=['date'])

# 删除缺失值
data.dropna(inplace=True)

# 只选择需要的特征，排除 'change' 列和 'date' 列
features = ['capacity', 'turnover', 'open', 'high', 'low', 'close', 'transaction']
data = data[features]

# 创建标签：如果下一天的收盘价高于当天，则标记为1，否则为0
data['label'] = ((data['high'].shift(-1) - data['open'].shift(-1)) / data['close'] > 0.02).astype(int)

# 删除最后一行，因为没有下一天的收盘价
data = data[:-1]

# 分离特征和标签
feature_columns = ['capacity', 'turnover', 'open', 'high', 'low', 'close', 'transaction']
X = data[feature_columns].values
y = data['label'].values

# 数据前处理-正则化
scaler = MinMaxScaler(feature_range=(0, 1))
X_scaled = scaler.fit_transform(X)

# 训练集和测试集的比例 8:2
train_data_len = int(len(X_scaled) * 0.8)

# 创建训练数据
X_train = []
y_train = []
for i in range(30, train_data_len):
    X_train.append(X_scaled[i-30:i, :])  # 使用过去30天的所有特征
    y_train.append(y[i])

X_train, y_train = np.array(X_train), np.array(y_train)

# 创建测试数据
X_test = []
y_test = []
for i in range(train_data_len, len(X_scaled) - 30):
    X_test.append(X_scaled[i-30:i, :])  # 使用过去30天的所有特征
    y_test.append(y[i])

X_test, y_test = np.array(X_test), np.array(y_test)

# 重新调整数据的形状以输入LSTM模型
# 形状应该为 (样本数, 时间步长, 特征数)
# 这里已经是这样，不需要额外的reshape
# 但为了确保，重新调整形状
X_train = np.reshape(X_train, (X_train.shape[0], X_train.shape[1], len(feature_columns)))
X_test = np.reshape(X_test, (X_test.shape[0], X_test.shape[1], len(feature_columns)))

# 计算类别权重
class_weights_values = class_weight.compute_class_weight(
    class_weight='balanced',
    classes=np.unique(y_train),
    y=y_train
)
class_weights = {i: class_weights_values[i] for i in range(len(class_weights_values))}

# 构建模型
model = Sequential()
model.add(LSTM(units=50, return_sequences=True, input_shape=(X_train.shape[1], X_train.shape[2])))
model.add(LSTM(units=50))
model.add(Dense(units=25))
model.add(Dense(units=1, activation='sigmoid'))  # 使用sigmoid激活函数进行二分类

# 编译模型
model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

# 训练模型，应用类别权重
history = model.fit(
    X_train, y_train,
    batch_size=32,  # 建议使用更大的batch_size以提高训练效率
    epochs=20,
    validation_data=(X_test, y_test),
    class_weight=class_weights,
    verbose=1
)

# 预测
predictions = model.predict(X_test)
predictions = (predictions > 0.5).astype(int).flatten()

# 评估模型
print("Classification Report:")
print(classification_report(y_test, predictions))
print("Confusion Matrix:")
print(confusion_matrix(y_test, predictions))
print("Accuracy:", accuracy_score(y_test, predictions))

# # 可视化结果
# # 如果原始数据中有日期，可以用日期进行可视化
# # 假设数据按日期排序
# plt.figure(figsize=(16, 8))
# plt.plot(range(len(y_test)), y_test, color='blue', label='Actual')
# plt.plot(range(len(predictions)), predictions, color='red', label='Predicted')
# plt.title('Stock Price Movement Prediction')
# plt.xlabel('Time')
# plt.ylabel('Movement (0=Down, 1=Up)')
# plt.legend()
# plt.show()
