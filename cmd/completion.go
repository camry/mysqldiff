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

  $ source <(mysqldiff completion bash)

  # To load completions for each session, execute once:
  # Linux:
  $ mysqldiff completion bash > /etc/bash_completion.d/mysqldiff && source /etc/bash_completion.d/mysqldiff
  # macOS:
  $ mysqldiff completion bash > /usr/local/etc/bash_completion.d/mysqldiff

Zsh:

  # If shell completion is not already enabled in your environment,
  # you will need to enable it.  You can execute the following once:

  $ echo "autoload -U compinit; compinit" >> ~/.zshrc

  # To load completions for each session, execute once:
  $ mysqldiff completion zsh > "${fpath[1]}/_mysqldiff"

  # You will need to start a new shell for this setup to take effect.

fish:

  $ mysqldiff completion fish | source

  # To load completions for each session, execute once:
  $ mysqldiff completion fish > ~/.config/fish/completions/mysqldiff.fish

PowerShell:

  PS> mysqldiff completion powershell | Out-String | Invoke-Expression

  # To load completions for every new session, run:
  PS> mysqldiff completion powershell > mysqldiff.ps1
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
