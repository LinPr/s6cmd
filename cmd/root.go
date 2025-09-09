/*
Copyright © 2025 LinQinyi
*/
package cmd

import (
	"fmt"
	"os"
	"sync"

	"github.com/LinPr/s6cmd/cmd/du"
	"github.com/LinPr/s6cmd/cmd/get"
	"github.com/LinPr/s6cmd/cmd/ls"
	"github.com/LinPr/s6cmd/cmd/mb"
	"github.com/LinPr/s6cmd/cmd/put"
	"github.com/LinPr/s6cmd/cmd/rm"
	"github.com/LinPr/s6cmd/cmd/stat"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var ConfigFile string

type Options struct {
}

func NewOptions() *Options {
	return &Options{}
}

func (o *Options) complete() error {
	// 使用 viper 获取到最终生效的配置 flag > env > config > default
	return nil
}

func (o *Options) validate() error {
	if err := validator.New().Struct(o); err != nil {
		return err
	}

	return nil
}

func (o *Options) run() error {
	return nil
}

// RootCmd represents the base command when called without any subcommands
func NewRootCmd() *cobra.Command {
	o := NewOptions()
	cmd := &cobra.Command{
		Use:     "s6cmd [command] [arguments...]",
		Short:   "S6cmd is a tool for managing objects in Amazon S3 storage",
		Example: "Find more infomation at README.md",
		// Long:  `Find more infomation at README.md`,
		// SuggestFor: ,
		// Uncomment the following line if your bare application
		// has an action associated with it:
		Run: func(cmd *cobra.Command, args []string) {
			if err := o.complete(); err != nil {
				fmt.Fprintf(os.Stderr, "err: %v\n", err)
				return
			}
			if err := o.validate(); err != nil {
				fmt.Fprintf(os.Stderr, "err: %v\n", err)
				return
			}
			if err := o.run(); err != nil {
				fmt.Fprintf(os.Stderr, "err: %v\n", err)
				return
			}
			cmd.Help()
		},
	}
	homeDir, _ := os.UserHomeDir()
	defaultConfigPath := fmt.Sprintf("%s/.s6cmd.yaml", homeDir)
	cmd.PersistentFlags().StringVar(&ConfigFile, "config", "", "default to "+defaultConfigPath)

	if err := viper.BindEnv("config", "S6CMD_CONFIG"); err != nil {
		panic(err)
	}

	registerSubCommands(cmd)
	return cmd
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the RootCmd.
func Execute() {
	once := sync.Once{}
	cobra.OnInitialize(func() {
		once.Do(func() { initConfig(ConfigFile) })
	})

	rootCmd := NewRootCmd()

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "err: %v\n", err)
		os.Exit(1)
	}

}

// initConfig reads in config file and ENV variables if set.
func initConfig(configFile string) {
	if configFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(configFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".fxops-cmd" (without extension).
		viper.AddConfigPath(home)
		// viper.SetConfigFile()
		viper.SetConfigType("yaml")
		viper.SetConfigName("s6cmd")
	}
	// read in environment variables that match
	viper.AutomaticEnv()

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err != nil {
		fmt.Fprintf(os.Stderr, "%s \n", err)
		return
	} else {
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	}

}

func registerSubCommands(cmd *cobra.Command) {
	cmd.AddCommand(ls.NewLsCmd())
	cmd.AddCommand(mb.NewMbCmd())
	cmd.AddCommand(rm.NewRmCmd())
	cmd.AddCommand(get.NewGetCmd())
	cmd.AddCommand(put.NewPutCmd())
	cmd.AddCommand(stat.NewStatCmd())
	cmd.AddCommand(du.NewDuCmd())
}
