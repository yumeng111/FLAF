class PlotTask(Task, HTCondorWorkflow, law.LocalWorkflow):
    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 2.0)
    n_cpus      = copy_param(HTCondorWorkflow.n_cpus, 1)

    def workflow_requires(self):        
        merge_map = MergeTask.req(self, branch=-1, branches=(), customisations=self.customisations).create_branch_map()
        return {"merge": MergeTask.req(self,branches=tuple(merge_map.keys()),customisations=self.customisations,)}
    
    def create_branch_map(self):
        branches = {}
        merge_map = MergeTask.req(self, branch=-1, branches=(), customisations=self.customisations).create_branch_map()

        for k, (_, (var, _)) in enumerate(merge_map.items()):
            branches[k] = var
        return branches

    def requires(self):
        var = self.branch_data

        merge_map = MergeTask.req(self, branch=-1, branches=(), customisations=self.customisations).create_branch_map()
        merge_branch = next(br for br, (v, _) in merge_map.items() if v == var)

        return MergeTask.req(self,branch=merge_branch,customisations=self.customisations,max_runtime=MergeTask.max_runtime._default,)

    def output(self):
        var  = self.branch_data
        flag_file= os.path.join(self.version, self.period, "plots", var, ".done")
        return self.remote_target(flag_file, fs=self.fs_plots)
    
    def run(self):
        var   = self.branch_data                   
        era   = self.period                        
        ver   = self.version
        customisation_dict = getCustomisationSplit(self.customisations)
        
        channels = customisation_dict['channels'] if 'channels' in customisation_dict else self.global_params['channelSelection']
        if isinstance(channels, str):
            channels = channels.split(',')
        
        base_cats = self.global_params.get('categories') or []
        boosted_cats = self.global_params.get('boosted_categories') or []
        categories = base_cats + boosted_cats
        if isinstance(categories, str):
            categories = categories.split(',')

        plotter = os.path.join(self.ana_path(), "FLAF", "Analysis", "HistPlotter.py")

        plot_unc = customisation_dict['plot_unc'] == 'True' if 'plot_unc' in customisation_dict.keys() else self.global_params.get('plot_unc', True)
        if plot_unc:
            remote_in = self.remote_target(os.path.join(ver, era, "merged", var, "tmp", f"all_histograms_{var}_hadded.root"),fs=self.fs_histograms,)
        else:
            remote_in = self.input()
        with remote_in.localize("r") as local_input:
            infile = local_input.path
            print("Loading fname", infile)      
            for ch in channels:
                for cat in categories:
                    rel_path = os.path.join(self.version, self.period, "plots", var, cat, f"HHbbtautau_{ch}_{var}_StackPlot.pdf")
                    with self.remote_target(rel_path, fs=self.fs_plots).localize("w") as local_pdf:
                        out_pdf = local_pdf.path
                        want_data = (
                            var != "MT2"
                            and (
                                ch in ["eE", "eMu", "muMu"] or (ch in ["eTau", "muTau", "tauTau"] and cat == "inclusive")
                            )
                        )
                        cmd = [
                            "python3", plotter,
                            "--inFile",      infile,
                            "--outFile",     out_pdf,
                            "--bckgConfig",  os.path.join(self.ana_path(), self.global_params["analysis_config_area"], "background_samples.yaml"),
                            "--globalConfig",os.path.join(self.ana_path(), self.global_params["analysis_config_area"], "global.yaml"),
                            "--sigConfig",   os.path.join(self.ana_path(), self.global_params["analysis_config_area"], era, "samples.yaml"),
                            "--var",         var,
                            "--category",    cat,
                            "--channel",     ch,
                            "--year",        era,
                        ]
                        if want_data:
                            cmd.append("--wantData")
                        plot_wantSignals = customisation_dict['plot_wantSignals'].lower() == 'true' if 'plot_wantSignals' in customisation_dict else self.global_params.get('plot_wantSignals', False)
                        plot_wantQCD = customisation_dict['plot_wantQCD'].lower() == 'true' if 'plot_wantQCD' in customisation_dict else self.global_params.get('plot_wantQCD', False)
                        plot_rebin = customisation_dict['plot_rebin'].lower() == 'true' if 'plot_rebin' in customisation_dict else self.global_params.get('plot_rebin', False)
                        plot_analysis = customisation_dict['plot_analysis'] if 'plot_analysis' in customisation_dict else self.global_params.get('plot_analysis', "")
                        if plot_wantSignals:
                            cmd.append("--wantSignals")
                        if plot_wantQCD:
                            cmd += ["--wantQCD", "true"]
                        if plot_rebin:
                            cmd += ["--rebin", "true"]
                        cmd += ["--analysis", plot_analysis]
                        ps_call(cmd, verbose=1)
            with self.output().localize("w") as local_flag_file:
                with local_flag_file.open("w") as f:
                    f.write("done\n") 