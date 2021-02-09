package plugins

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/plugins/defaults"
)

type SchedulingPlugins struct {
	applicationsPlugin ApplicationsPlugin
	requestsPlugin     RequestsPlugin
	nodeManagerPlugin  NodeManagerPlugin

	sync.RWMutex
}

var plugins SchedulingPlugins

func init() {
	plugins = SchedulingPlugins{}
	// initialize default plugins
	Init(NewRegistry(), GetDefaultPluginsConfig())
}

func RegisterPlugin(plugin interface{}) error {
	plugins.Lock()
	defer plugins.Unlock()
	registeredPluginNames := make([]string, 0)
	if t, ok := plugin.(RequestsPlugin); ok {
		plugins.requestsPlugin = t
		registeredPluginNames = append(registeredPluginNames, "RequestsPlugin")
	}
	if t, ok := plugin.(ApplicationsPlugin); ok {
		plugins.applicationsPlugin = t
		registeredPluginNames = append(registeredPluginNames, "ApplicationsPlugin")
	}
	if t, ok := plugin.(NodeManagerPlugin); ok {
		plugins.nodeManagerPlugin = t
		registeredPluginNames = append(registeredPluginNames, "NodeManagerPlugin")
	}
	if len(registeredPluginNames) == 0 {
		return fmt.Errorf("%s can't be registered as a scheduling plugin", reflect.TypeOf(plugin).String())
	}
	log.Logger().Info("registered plugin(s)", zap.String("type", reflect.TypeOf(plugin).String()),
		zap.Any("registeredPluginNames", registeredPluginNames))
	return nil
}

func GetRequestsPlugin() RequestsPlugin {
	plugins.RLock()
	defer plugins.RUnlock()

	return plugins.requestsPlugin
}

func GetApplicationsPlugin() ApplicationsPlugin {
	plugins.RLock()
	defer plugins.RUnlock()

	return plugins.applicationsPlugin
}

func GetNodeManagerPlugin() NodeManagerPlugin {
	plugins.RLock()
	defer plugins.RUnlock()

	return plugins.nodeManagerPlugin
}

// This plugin is responsible for creating new instances of Applications.
type ApplicationsPlugin interface {
	interfaces.Plugin
	// return a new instance of Applications
	NewApplications(queue interfaces.Queue) interfaces.Applications
}

// This plugin is responsible for creating new instances of Requests.
type RequestsPlugin interface {
	interfaces.Plugin
	// return a new instance of requests
	NewRequests() interfaces.Requests
}

// This plugin is responsible for creating new instances of NodeManager.
type NodeManagerPlugin interface {
	interfaces.Plugin
	// return a new instance of NodeManager,
	// if failed to create with specified arguments, log the error then return default node manager.
	NewNodeManager(partition interfaces.Partition, args interface{}) interfaces.NodeManager
	// verify arguments
	Validate(args interface{}) error
}

func GetDefaultPluginsConfig() *configs.PluginsConfig {
	return &configs.PluginsConfig{
		Plugins: []*configs.PluginConfig{
			{
				Name: defaults.DefaultApplicationsPluginName,
			}, {
				Name: defaults.DefaultRequestsPluginName,
			}, {
				Name: defaults.DefaultNodeManagerPluginName,
			},
		},
	}
}

func Init(registry Registry, pluginsConfig *configs.PluginsConfig) error {
	if pluginsConfig == nil {
		return fmt.Errorf("plugins config is nil")
	}
	// init requests plugin
	for _, pluginConfig := range pluginsConfig.Plugins {
		plugin, err := registry.GeneratePlugin(pluginConfig.Name,
			pluginConfig.Args)
		if err != nil {
			return err
		}
		err = RegisterPlugin(plugin)
		if err != nil {
			return err
		}
	}
	return nil
}
