-- 确保Canal用户使用正确的认证插件
ALTER USER 'canal'@'%' IDENTIFIED WITH mysql_native_password BY 'canal';

-- 授予完整权限
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT, RELOAD, SUPER 
ON *.* TO 'canal'@'%';

FLUSH PRIVILEGES;