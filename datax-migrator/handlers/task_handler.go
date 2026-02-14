package handlers

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"datax-migrator/services"
)

// ListTasks 获取任务执行记录列表
func ListTasks(jobService *services.JobService) gin.HandlerFunc {
	return func(c *gin.Context) {
		page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
		pageSize, _ := strconv.Atoi(c.DefaultQuery("pageSize", "20"))

		// 这里简化处理，实际应该从服务层获取
		c.JSON(http.StatusOK, gin.H{
			"tasks":    []interface{}{},
			"total":    0,
			"page":     page,
			"pageSize": pageSize,
		})
	}
}

// GetTask 获取任务执行详情
func GetTask(jobService *services.JobService) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, err := strconv.ParseUint(c.Param("id"), 10, 32)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "无效的任务ID"})
			return
		}

		task, err := jobService.GetTask(uint(id))
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"task": task,
		})
	}
}

// GetTaskLogs 获取任务日志
func GetTaskLogs(jobService *services.JobService) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, err := strconv.ParseUint(c.Param("id"), 10, 32)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "无效的任务ID"})
			return
		}

		logs, err := jobService.GetTaskLogs(uint(id))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"logs": logs,
		})
	}
}
