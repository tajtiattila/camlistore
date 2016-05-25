/*
Copyright 2016 The Camlistore Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/client"
	"camlistore.org/pkg/cmdmain"
	"camlistore.org/pkg/osutil"
	"camlistore.org/pkg/schema"
	"camlistore.org/pkg/schema/nodeattr"
	"camlistore.org/pkg/search"
)

type fileMetaCmd struct {
	server string

	verbose bool // whether to describe each blob.

	cl     *client.Client // client used for the describe requests.
	signer *schema.Signer // signer used for new claims
}

func init() {
	cmdmain.RegisterCommand("filemeta", func(flags *flag.FlagSet) cmdmain.CommandRunner {
		cmd := new(fileMetaCmd)
		flags.StringVar(&cmd.server, "server", "", "Server to search. "+serverFlagHelp)
		flags.BoolVar(&cmd.verbose, "verbose", false, "Be verbose.")
		return cmd
	})

	osutil.AddSecretRingFlag()
}

func (c *fileMetaCmd) Describe() string {
	return "Generate missing file permanode attrs on the server."
}

func (c *fileMetaCmd) Usage() {
	fmt.Fprintf(os.Stderr, "Usage: camtool [globalopts] filemeta [filemetaopts] \n")
}

func (c *fileMetaCmd) Examples() []string {
	return nil
}

func (c *fileMetaCmd) RunCommand(args []string) error {
	if len(args) != 0 {
		return cmdmain.UsageError("doesn't take arguments")
	}

	c.cl = newClient(c.server)

	var err error
	c.signer, err = c.cl.Signer()
	if err != nil {
		return err
	}

	req := &search.SearchQuery{
		Constraint: &search.Constraint{
			Permanode: &search.PermanodeConstraint{
				Attr: nodeattr.CamliContent,
				ValueInSet: &search.Constraint{
					File: &search.FileConstraint{
						IsImage: true,
					},
				},
			},
		},
		Describe: &search.DescribeRequest{
			Rules: []*search.DescribeRule{
				{Attrs: []string{nodeattr.CamliContent}},
			},
		},
	}

	for {
		sr, err := c.cl.Query(req)
		if err != nil {
			return err
		}

		for _, db := range sr.Describe.Meta {
			if content, ok := db.ContentRef(); ok {
				c.addFilemeta(db.BlobRef, content)
			}
		}

		if sr.Continue == "" {
			break
		}
		req.Continue = sr.Continue
	}
	return nil
}

func (c *fileMetaCmd) addFilemeta(pn, content blob.Ref) {
	fr, err := schema.NewFileReader(c.cl, content)
	if err != nil {
		log.Printf("failed fetch %v: %v", content, err)
		return
	}
	defer fr.Close()

	claimTime := fr.ModTime()
	if claimTime.IsZero() {
		log.Printf("file %v has zero modtime", content)
		return
	}

	fm, err := schema.FileFileMeta(fr)
	if err != nil {
		// content has no metadata
		if c.verbose {
			log.Printf("no metadata for %v: %v", content, err)
		}
		return
	}

	// TODO(ata): merge with camput/files.go
	errch := make(chan error)
	n := 0
	for k, vs := range fm.Attr {
		for _, v := range vs {
			go func(k, v string) {
				m := schema.NewAddAttributeClaim(pn, k, v)
				m.SetClaimDate(claimTime)
				signed, err := m.SignAt(c.signer, claimTime)
				if err != nil {
					errch <- fmt.Errorf("Failed to sign %s claim: %v", k, err)
					return
				}
				put, err := c.cl.Upload(client.NewUploadHandleFromString(signed))
				if err != nil {
					errch <- fmt.Errorf("Error uploading permanode's %v attribute %v: %v", k, v, err)
					return
				}
				if c.verbose {
					fmt.Printf("%v %q %q -> %v\n", pn, k, v, put.BlobRef)
				}
				errch <- nil
			}(k, v)

			n++
		}
	}

	for i := 0; i < n; i++ {
		if e := <-errch; e != nil {
			log.Println(e)
		}
	}
}
