package handlers

import (
	"log"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"datax-migrator/models"
	"datax-migrator/services"
)

// CreateJob 创建迁移任务
func CreateJob(jobService *services.JobService) gin.HandlerFunc {
	return func(c *gin.Context) {
		var job models.MigrationJob
		if err := c.ShouldBindJSON(&job); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "请求参数错误", "details": err.Error()})
			return
		}

		if err := jobService.CreateJob(&job); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusCreated, gin.H{
			"message": "任务创建成功",
			"job":     job,
		})
	}
}

// UpdateJob 更新迁移任务
func UpdateJob(jobService *services.JobService) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, err := strconv.ParseUint(c.Param("id"), 10, 32)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "无效的任务ID"})
			return
		}

		var job models.MigrationJob
		if err := c.ShouldBindJSON(&job); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "请求参数错误", "details": err.Error()})
			return
		}

		job.ID = uint(id)
		if err := jobService.UpdateJob(&job); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"message": "任务更新成功",
			"job":     job,
		})
	}
}

// DeleteJob 删除迁移任务
func DeleteJob(jobService *services.JobService) gin.HandlerFunc {
	return func(c *gin.Context) {
		idStr := c.Param("id")
		log.Printf("接收到的删除请求 ID 原始值: %s", idStr) // 记录日志便于调试

		id, err := strconv.ParseUint(idStr, 10, 32)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "无效的任务ID"})
			return
		}

		if err := jobService.DeleteJob(uint(id)); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"message": "任务删除成功"})
	}
}

// GetJob 获取任务详情
func GetJob(jobService *services.JobService) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, err := strconv.ParseUint(c.Param("id"), 10, 32)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "无效的任务ID"})
			return
		}

		job, err := jobService.GetJob(uint(id))
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"job": job})
	}
}

// ListJobs 获取任务列表（支持分页和过滤）
func ListJobs(jobService *services.JobService) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 解析并验证 page
		page, err := strconv.Atoi(c.DefaultQuery("page", "1"))
		if err != nil || page < 1 {
			page = 1
		}

		// 解析并验证 pageSize
		pageSize, err := strconv.Atoi(c.DefaultQuery("pageSize", "20"))
		if err != nil || pageSize < 1 {
			pageSize = 20
		}
		if pageSize > 100 {
			pageSize = 100 // 限制最大每页条数
		}

		// 收集过滤条件
		filters := make(map[string]interface{})
		if status := c.Query("status"); status != "" {
			filters["status"] = status
		}
		if name := c.Query("name"); name != "" {
			filters["name"] = name
		}

		jobs, total, err := jobService.ListJobs(page, pageSize, filters)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// 计算总页数
		totalPage := (total + int64(pageSize) - 1) / int64(pageSize)

		c.JSON(http.StatusOK, gin.H{
			"jobs":      jobs,
			"total":     total,
			"page":      page,
			"pageSize":  pageSize,
			"totalPage": totalPage,
		})
	}
}

// RunJob 执行迁移任务
func RunJob(jobService *services.JobService) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, err := strconv.ParseUint(c.Param("id"), 10, 32)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "无效的任务ID"})
			return
		}

		task, err := jobService.RunJob(uint(id))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"message": "任务开始执行",
			"task":    task,
		})
	}
}

// StopJob 停止迁移任务
func StopJob(jobService *services.JobService) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, err := strconv.ParseUint(c.Param("id"), 10, 32)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "无效的任务ID"})
			return
		}

		if err := jobService.StopJob(uint(id)); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"message": "任务已停止"})
	}
}

// GetJobTasks 获取指定任务的所有执行记录（分页）
func GetJobTasks(jobService *services.JobService) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, err := strconv.ParseUint(c.Param("id"), 10, 32)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "无效的任务ID"})
			return
		}

		// 解析并验证 page
		page, err := strconv.Atoi(c.DefaultQuery("page", "1"))
		if err != nil || page < 1 {
			page = 1
		}

		// 解析并验证 pageSize
		pageSize, err := strconv.Atoi(c.DefaultQuery("pageSize", "10"))
		if err != nil || pageSize < 1 {
			pageSize = 10
		}
		if pageSize > 100 {
			pageSize = 100 // 限制最大每页条数
		}

		tasks, total, err := jobService.GetJobTasks(uint(id), page, pageSize)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"tasks":    tasks,
			"total":    total,
			"page":     page,
			"pageSize": pageSize,
		})
	}
}
