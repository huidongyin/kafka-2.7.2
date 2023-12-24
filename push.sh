#!/bin/bash

# 提示用户输入提交信息
read -p "Enter commit message: " commit_message

# 添加文件到暂存区
git add .

# 提交文件并使用用户输入的信息
git commit -m "$commit_message"

# 推送到远程仓库
git push

