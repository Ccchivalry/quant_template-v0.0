# quant_template-v0.0

# 结构说明
-code
  -main：程序运行总接口
  -scripts：程序脚本（存放个性化代码）
    -config：路径、数据库配置
    -db:  数据库相关操作模块
      -sqls.yaml:  sql语句统一存放地址
      -ConnectingDatabase：数据库操作类
    -utils：工具类模块
      -log_utils:  日志操作工具

-data: 存放数据

-doc：存放项目相关文档

-logs：日志存放地址

README.md

requirement.txt: 记录所有依赖包及其精确的版本号，以便新环境部署。生成方式：pip install pipreqs；pipreqs . --encoding=utf8 --force；