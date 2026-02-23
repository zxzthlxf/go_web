package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"datax-migrator/services"
)

// GetReaderPlugins 获取读取插件列表
func GetReaderPlugins(dataxService *services.DataXService) gin.HandlerFunc {
	return func(c *gin.Context) {
		plugins, err := dataxService.GetReaderPlugins()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"plugins": plugins})
	}
}

// GetWriterPlugins 获取写入插件列表
func GetWriterPlugins(dataxService *services.DataXService) gin.HandlerFunc {
	return func(c *gin.Context) {
		plugins, err := dataxService.GetWriterPlugins()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"plugins": plugins})
	}
}

// GenerateDataXConfig 生成DataX配置（未实现）
// 该接口需要完整的任务信息，请通过创建任务功能自动生成配置。
func GenerateDataXConfig(dataxService *services.DataXService) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req struct {
			SourceType string                 `json:"source_type"`
			TargetType string                 `json:"target_type"`
			Source     map[string]interface{} `json:"source"`
			Target     map[string]interface{} `json:"target"`
			Settings   map[string]interface{} `json:"settings"`
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "请求参数错误", "details": err.Error()})
			return
		}

		// 该接口尚未实现，返回 501 状态码
		c.JSON(http.StatusNotImplemented, gin.H{
			"error":   "配置生成接口未实现",
			"message": "请通过创建任务功能自动生成DataX配置",
		})
	}
}

// ValidateDataXConfig 验证DataX配置
func ValidateDataXConfig(dataxService *services.DataXService) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req struct {
			Config string `json:"config"`
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "请求参数错误", "details": err.Error()})
			return
		}

		if req.Config == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "配置不能为空"})
			return
		}

		valid, message := dataxService.ValidateConfig(req.Config)
		c.JSON(http.StatusOK, gin.H{
			"valid":   valid,
			"message": message,
		})
	}
}
