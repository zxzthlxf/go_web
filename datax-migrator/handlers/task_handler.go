package handlers

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"datax-migrator/services"
)

// ListTasks 获取任务执行记录列表（全局，支持分页）
// 注意：该接口需要服务层实现 ListAllTasks 方法，当前返回未实现错误。
func ListTasks(jobService *services.JobService) gin.HandlerFunc {
	return func(c *gin.Context) {
		page, err := strconv.Atoi(c.DefaultQuery("page", "1"))
		if err != nil || page < 1 {
			page = 1
		}
		pageSize, err := strconv.Atoi(c.DefaultQuery("pageSize", "20"))
		if err != nil || pageSize < 1 || pageSize > 100 {
			pageSize = 20
		}

		// TODO: 调用 jobService.ListAllTasks(page, pageSize) 获取全局任务列表
		// 由于服务层未实现该方法，暂时返回 501 状态码
		c.JSON(http.StatusNotImplemented, gin.H{
			"error":   "全局任务列表接口未实现",
			"message": "请先实现服务层 ListAllTasks 方法",
		})
	}
}

// GetTask 获取指定任务执行详情
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

		c.JSON(http.StatusOK, gin.H{"task": task})
	}
}

// GetTaskLogs 获取指定任务的执行日志
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

		c.JSON(http.StatusOK, gin.H{"logs": logs})
	}
}
