# This repo allows to automatically switch to the conda environment when the user navigates to the project directory.

To make this work follow these steps:

The environment named in `.condaenv` (`trunk-canton`) must exist first. Otherwise the hook has
nothing to activate, and `cd` only prints `Auto-deactivating Conda environment: base`:

```bash
conda create -n trunk-canton python=3.10
conda activate trunk-canton
pip install -e ".[dev]"
```

If you already have conda, your .bashrc or .zshrc file should have a lines like this:

```bash
# >>> conda initialize >>>
# !! Contents within this block are managed by 'conda init' !!
__conda_setup="$('/home/$USER/miniconda3/bin/conda' 'shell.zsh' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]; then
    eval "$__conda_setup"
else
    if [ -f "/home/$USER/miniconda3/etc/profile.d/conda.sh" ]; then
        . "/home/$USER/miniconda3/etc/profile.d/conda.sh"
    else
        export PATH="/home/$USER/miniconda3/bin:$PATH"
    fi
fi
unset __conda_setup
# <<< conda initialize <<<
```

To make the autoconda work, add the following lines to the end of your .bashrc or .zshrc file, after conda initialization:

```bash
# Automatic Conda Environment Activation

autoload -U add-zsh-hook

auto_conda_env() {
  local dir="$PWD"
  local prev_env="$CONDA_DEFAULT_ENV"
  local conda_env_name=""
  local found_condaenv_file=""

  while [[ "$dir" != "/" ]]; do
    if [[ -f "$dir/.condaenv" ]]; then
      found_condaenv_file="$dir/.condaenv"
      conda_env_name="$(cat "$found_condaenv_file")"
      break
    fi
    dir="$(dirname "$dir")"
  done

  if [[ -n "$conda_env_name" ]]; then
    if [[ "$CONDA_DEFAULT_ENV" != "$conda_env_name" ]]; then
      if [[ -n "$CONDA_DEFAULT_ENV" ]]; then
        conda deactivate
      fi
      echo "Auto-activating Conda environment: $conda_env_name (from $found_condaenv_file)"
      conda activate "$conda_env_name"
    fi
  else
    if [[ -n "$CONDA_DEFAULT_ENV" ]]; then
      echo "Auto-deactivating Conda environment: $CONDA_DEFAULT_ENV"
      conda deactivate
    fi
  fi
}

add-zsh-hook chpwd auto_conda_env
# Run once when the shell starts
auto_conda_env
```

Source your .zshrc or.bashrc file:

```bash
source ~/.zshrc
```
# Test automatic conda environment activation

While in project directory you should see following:
    
```bash
    cd ~/Projects/trunk-canton                                                                                                        
Auto-activating Conda environment: trunk-canton (from /home/$USER/Projects/trunk-canton/.condaenv)
```
And when you echo following variables you should see the following:

```bash
echo $CONDA_DEFAULT_ENV                                                                                                    

trunk-canton
         
```

or alternatively, you can use the following command to check the active python environment:

```bash
which python3 
```

Output will be path to your conda installation.

# Automatic deactivation

When you leave the project directory, the conda environment will be deactivated automatically.
For example:

```bash
cd ~
Auto-deactivating Conda environment: trunk-canton
```

and when you $CONDA_DEFAULT_ENV or check which or where python, you will see your default python installation.