package cmd

import (
	"github.com/spf13/cobra"
	"os"
)

var completionCmd = &cobra.Command{
	Use:   "completion [bash|zsh|fish|powershell]",
	Short: "Generate completion script",
	Long: `To load completions:

Bash:

  $ source <(game-cli completion bash)

  # To load completions for each session, execute once:
  # Linux:
  $ game-cli completion bash > /etc/bash_completion.d/game-cli && source /etc/bash_completion.d/game-cli
  # macOS:
  $ game-cli completion bash > /usr/local/etc/bash_completion.d/game-cli

Zsh:

  # If shell completion is not already enabled in your environment,
  # you will need to enable it.  You can execute the following once:

  $ echo "autoload -U compinit; compinit" >> ~/.zshrc

  # To load completions for each session, execute once:
  $ game-cli completion zsh > "${fpath[1]}/_game-cli"

  # You will need to start a new shell for this setup to take effect.

fish:

  $ game-cli completion fish | source

  # To load completions for each session, execute once:
  $ game-cli completion fish > ~/.config/fish/completions/game-cli.fish

PowerShell:

  PS> game-cli completion powershell | Out-String | Invoke-Expression

  # To load completions for every new session, run:
  PS> game-cli completion powershell > game-cli.ps1
  # and source this file from your PowerShell profile.
`,
	DisableFlagsInUseLine: true,
	Hidden:                true,
	ValidArgs:             []string{"bash", "zsh", "fish", "powershell"},
	Args:                  cobra.ExactValidArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		var err error

		switch args[0] {
		case "bash":
			err = cmd.Root().GenBashCompletion(os.Stdout)
		case "zsh":
			err = cmd.Root().GenZshCompletion(os.Stdout)
		case "fish":
			err = cmd.Root().GenFishCompletion(os.Stdout, true)
		case "powershell":
			err = cmd.Root().GenPowerShellCompletionWithDesc(os.Stdout)
		}

		cobra.CheckErr(err)
	},
}
