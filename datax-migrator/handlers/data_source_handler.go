package handlers

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"datax-migrator/models"
	"datax-migrator/services"
)

// CreateDataSource 创建数据源
func CreateDataSource(dsService *services.DataSourceService) gin.HandlerFunc {
	return func(c *gin.Context) {
		var ds models.DataSource
		if err := c.ShouldBindJSON(&ds); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "请求参数错误", "details": err.Error()})
			return
		}

		if err := dsService.CreateDataSource(&ds); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusCreated, gin.H{
			"message":     "数据源创建成功",
			"data_source": ds,
		})
	}
}

// UpdateDataSource 更新数据源
func UpdateDataSource(dsService *services.DataSourceService) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, err := strconv.ParseUint(c.Param("id"), 10, 32)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "无效的数据源ID"})
			return
		}

		var ds models.DataSource
		if err := c.ShouldBindJSON(&ds); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "请求参数错误", "details": err.Error()})
			return
		}

		ds.ID = uint(id)
		if err := dsService.UpdateDataSource(&ds); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"message":     "数据源更新成功",
			"data_source": ds,
		})
	}
}

// DeleteDataSource 删除数据源
func DeleteDataSource(dsService *services.DataSourceService) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, err := strconv.ParseUint(c.Param("id"), 10, 32)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "无效的数据源ID"})
			return
		}

		if err := dsService.DeleteDataSource(uint(id)); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"message": "数据源删除成功",
		})
	}
}

// GetDataSource 获取数据源详情
func GetDataSource(dsService *services.DataSourceService) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, err := strconv.ParseUint(c.Param("id"), 10, 32)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "无效的数据源ID"})
			return
		}

		ds, err := dsService.GetDataSource(uint(id))
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"data_source": ds,
		})
	}
}

// ListDataSources 获取数据源列表
func ListDataSources(dsService *services.DataSourceService) gin.HandlerFunc {
	return func(c *gin.Context) {
		dataSources, err := dsService.ListDataSources()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"data_sources": dataSources,
			"total":        len(dataSources),
		})
	}
}

// TestDataSource 测试数据源连接
func TestDataSource(dsService *services.DataSourceService) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, err := strconv.ParseUint(c.Param("id"), 10, 32)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "无效的数据源ID"})
			return
		}

		ds, err := dsService.GetDataSource(uint(id))
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		if err := dsService.TestDataSource(ds); err != nil {
			c.JSON(http.StatusOK, gin.H{
				"success": false,
				"message": "连接失败: " + err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "连接成功",
		})
	}
}

// GetDatabaseTables 获取数据库表列表
func GetDatabaseTables(dsService *services.DataSourceService) gin.HandlerFunc {
	return func(c *gin.Context) {
		dataSourceID, err := strconv.ParseUint(c.Query("data_source_id"), 10, 32)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "无效的数据源ID"})
			return
		}

		tables, err := dsService.GetDatabaseTables(uint(dataSourceID))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"tables": tables,
		})
	}
}

// GetTableColumns 获取表字段信息
func GetTableColumns(dsService *services.DataSourceService) gin.HandlerFunc {
	return func(c *gin.Context) {
		dataSourceID, err := strconv.ParseUint(c.Query("data_source_id"), 10, 32)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "无效的数据源ID"})
			return
		}

		tableName := c.Query("table_name")
		if tableName == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "表名不能为空"})
			return
		}

		columns, err := dsService.GetTableColumns(uint(dataSourceID), tableName)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"columns": columns,
		})
	}
}

// ExecuteQuery 执行SQL查询
func ExecuteQuery(dsService *services.DataSourceService) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req struct {
			DataSourceID uint   `json:"data_source_id"`
			Query        string `json:"query"`
			Limit        int    `json:"limit"`
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "请求参数错误"})
			return
		}

		if req.Query == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "查询语句不能为空"})
			return
		}

		if req.Limit <= 0 {
			req.Limit = 100
		}

		results, err := dsService.ExecuteQuery(req.DataSourceID, req.Query, req.Limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"results": results,
			"count":   len(results),
		})
	}
}
