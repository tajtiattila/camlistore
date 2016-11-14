/*
Copyright 2011 The Camlistore Authors

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

package search_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/httputil"
	"camlistore.org/pkg/index"
	"camlistore.org/pkg/index/indextest"
	"camlistore.org/pkg/osutil"
	. "camlistore.org/pkg/search"
	"camlistore.org/pkg/test"
	"camlistore.org/pkg/types/camtypes"
)

type indexMapper struct {
	*indextest.IndexDeps

	Refs map[string]blob.Ref
}

func (m *indexMapper) AddPermanode(key string, attrs ...string) blob.Ref {
	pn := m.NewPlannedPermanode(key)
	m.Refs[key] = pn
	for len(attrs) > 0 {
		k, v := attrs[0], attrs[1]
		attrs = attrs[2:]
		m.AddAttribute(pn, k, v)
	}
	return pn
}

func (m *indexMapper) AddFile(key string, nbytes int) blob.Ref {
	contents := strings.Repeat(key, nbytes/len(key)+1)[:nbytes]
	// use a file name that is not a blobRef to avoid remapping
	fileRef, wholeRef := m.UploadFile(strings.Replace(key, "-", "", 1), contents, time.Time{})
	m.Refs[key] = fileRef
	m.Refs["whole"+key] = wholeRef
	return fileRef
}

func (m *indexMapper) Ref(key string) blob.Ref {
	r, ok := m.Refs[key]
	if !ok {
		panic("key missing from indexMapper")
	}
	return r
}

var blobRefPattern = regexp.MustCompile(blob.Pattern)

func (m *indexMapper) Map(key string) string {
	return blobRefPattern.ReplaceAllStringFunc(key, func(s string) string {
		r, ok := m.Refs[s]
		if ok {
			return r.String()
		}
		return s
	})
}

func (m *indexMapper) MapUrl(urlStr string) string {
	u, err := url.Parse(urlStr)
	if err != nil {
		panic(err)
	}
	q := u.Query()
	for _, vv := range q {
		for i, v := range vv {
			vv[i] = m.Map(v)
		}
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func (m *indexMapper) MapMap(src map[string]interface{}) map[string]interface{} {
	dst := make(map[string]interface{})
	for k, v := range src {
		dst[m.Map(k)] = m.mapIntf(v)
	}
	return dst
}

func (m *indexMapper) mapIntf(src interface{}) interface{} {
	switch x := src.(type) {
	case map[string]interface{}:
		return m.MapMap(x)
	case []interface{}:
		res := make([]interface{}, len(x))
		for i, v := range x {
			res[i] = m.mapIntf(v)
		}
		return res
	case string:
		return m.Map(x)
	default:
		return src
	}
}

type handlerTest struct {
	// setup is responsible for populating the index before the
	// handler is invoked.
	//
	// An indexMapper is constructed and provided to setup.
	setup func(im *indexMapper) index.Interface

	name string // test name

	// References in query, postBody, want and wantDescribed
	// are replaced with references in the indexMapper.

	query    string // the HTTP path + optional query suffix after "camli/search/"
	postBody string // if non-nil, a POST request

	want map[string]interface{}
	// wantDescribed is a list of blobref strings that should've been
	// described in meta. If want is nil and this is non-zero length,
	// want is ignored.
	wantDescribed []string
}

func parseJSON(s string) map[string]interface{} {
	m := make(map[string]interface{})
	err := json.Unmarshal([]byte(s), &m)
	if err != nil {
		panic(err)
	}
	return m
}

// addToClockOrigin returns the given Duration added
// to test.ClockOrigin, in UTC, and RFC3339Nano formatted.
func addToClockOrigin(d time.Duration) string {
	return test.ClockOrigin.Add(d).UTC().Format(time.RFC3339Nano)
}

func handlerDescribeTestSetup(im *indexMapper) index.Interface {
	pn := im.AddPermanode("perma-123")
	f := im.AddFile("fakeref-232", 878)
	im.SetAttribute(pn, "camliContent", f.String())

	// Test deleting all attributes
	im.AddAttribute(pn, "wont-be-present", "x")
	im.AddAttribute(pn, "wont-be-present", "y")
	im.DelAttribute(pn, "wont-be-present", "")

	// Test deleting a specific attribute.
	im.AddAttribute(pn, "only-delete-b", "a")
	im.AddAttribute(pn, "only-delete-b", "b")
	im.AddAttribute(pn, "only-delete-b", "c")
	im.DelAttribute(pn, "only-delete-b", "b")

	return im.Index
}

// extends handlerDescribeTestSetup but adds a camliContentImage to pn.
func handlerDescribeTestSetupWithImage(im *indexMapper) index.Interface {
	handlerDescribeTestSetup(im)
	pn := im.Ref("perma-123")
	imageRef := im.AddFile("fakeref-789", 789)
	im.SetAttribute(pn, "camliContentImage", imageRef.String())
	return im.Index
}

// extends handlerDescribeTestSetup but adds various embedded references to other nodes.
func handlerDescribeTestSetupWithEmbeddedRefs(im *indexMapper) index.Interface {
	handlerDescribeTestSetup(im)
	pn := im.Ref("perma-123")
	c1 := im.AddPermanode("fakeref-01", "title", "fake01")
	c2 := im.AddPermanode("fakeref-02", "title", "fake02")
	c3 := im.AddPermanode("fakeref-03", "title", "fake03")
	c4 := im.AddPermanode("fakeref-04", "title", "fake04")
	c5 := im.AddPermanode("fakeref-05", "title", "fake05")
	c6 := im.AddPermanode("fakeref-06", "title", "fake06")
	im.SetAttribute(pn, c1.String(), "foo")
	im.SetAttribute(pn, "foo,"+c2.String()+"=bar", "foo")
	im.SetAttribute(pn, "foo:"+c3.String()+"?bar,"+c4.String(), "foo")
	im.SetAttribute(pn, "foo", c5.String())
	im.AddAttribute(pn, "bar", "baz")
	im.AddAttribute(pn, "bar", "monkey\n"+c6.String())
	return im.Index
}

var handlerTests = []handlerTest{
	{
		name:  "describe-missing",
		setup: func(im *indexMapper) index.Interface { return im.Index },
		query: "describe?blobref=eabfakeref-0555",
		want: parseJSON(`{
			"meta": {
			}
		}`),
	},

	{
		name: "describe-jpeg-blob",
		setup: func(im *indexMapper) index.Interface {
			im.AddFile("abfakeref-0555", 999)
			return im.Index
		},
		query: "describe?blobref=abfakeref-0555",
		want: parseJSON(`{
			"meta": {
				` + fileResult("abfakeref-0555", 999) + `
			}
		}`),
	},

	{
		name:  "describe-permanode",
		setup: handlerDescribeTestSetup,
		query: "describe",
		postBody: `{
 "blobref": "perma-123",
 "rules": [
    {"attrs": ["camliContent"]}
 ]
}`,
		want: parseJSON(`{
			"meta": {
				` + fileResult("fakeref-232", 878) + `,
				"perma-123": {
					"blobRef":   "perma-123",
					"camliType": "permanode",
					"permanode": {
						"attr": {
							"camliContent": [ "fakeref-232" ],
							"only-delete-b": [ "a", "c" ]
						},
						"modtime": "` + addToClockOrigin(8*time.Second) + `"
					}
				}
			}
		}`),
	},

	{
		name:  "describe-permanode-image",
		setup: handlerDescribeTestSetupWithImage,
		query: "describe",
		postBody: `{
 "blobref": "perma-123",
 "rules": [
    {"attrs": ["camliContent", "camliContentImage"]}
 ]
}`,
		want: parseJSON(`{
			"meta": {
				` + fileResult("fakeref-232", 878) + `,
				` + fileResult("fakeref-789", 789) + `,
				"perma-123": {
					"blobRef":   "perma-123",
					"camliType": "permanode",
					"permanode": {
						"attr": {
							"camliContent": [ "fakeref-232" ],
							"camliContentImage": [ "fakeref-789" ],
							"only-delete-b": [ "a", "c" ]
						},
						"modtime": "` + addToClockOrigin(9*time.Second) + `"
					}
				}
			}
		}`),
	},

	// TODO(bradfitz): we'll probably will want to delete or redo this
	// test when we remove depth=N support from describe.
	{
		name:  "describe-permanode-embedded-references",
		setup: handlerDescribeTestSetupWithEmbeddedRefs,
		query: "describe?blobref=perma-123&depth=2",
		want: parseJSON(`{
			"meta": {
				` + permResult("fakeref-01", "fake01", 9*time.Second) + `,
				` + permResult("fakeref-02", "fake02", 10*time.Second) + `,
				` + permResult("fakeref-03", "fake03", 11*time.Second) + `,
				` + permResult("fakeref-04", "fake04", 12*time.Second) + `,
				` + permResult("fakeref-05", "fake05", 13*time.Second) + `,
				` + permResult("fakeref-06", "fake06", 14*time.Second) + `,
				` + fileResult("fakeref-232", 878) + `,
				"perma-123": {
					"blobRef":   "perma-123",
					"camliType": "permanode",
					"permanode": {
						"attr": {
							"bar": [
								"baz",
								"monkey\nfakeref-06"
							],
							"fakeref-01": [
								"foo"
							],
							"camliContent": [
								"fakeref-232"
							],
							"foo": [
								"fakeref-05"
							],
							"foo,fakeref-02=bar": [
								"foo"
							],
							"foo:fakeref-03?bar,fakeref-04": [
								"foo"
							],
							"camliContent": [ "fakeref-232" ],
							"only-delete-b": [ "a", "c" ]
						},
						"modtime": "` + addToClockOrigin(20*time.Second) + `"
					}
				}
			}
		}`),
	},

	{
		name:  "describe-permanode-timetravel",
		setup: handlerDescribeTestSetup,
		query: "describe",
		postBody: `{
 "blobref": "perma-123",
 "at": "` + addToClockOrigin(3*time.Second) + `",
 "rules": [
    {"attrs": ["camliContent"]}
 ]
}`,
		want: parseJSON(`{
			"meta": {
				` + fileResult("fakeref-232", 878) + `,
				"perma-123": {
					"blobRef":   "perma-123",
					"camliType": "permanode",
					"permanode": {
						"attr": {
							"camliContent": [ "fakeref-232" ],
							"wont-be-present": [ "x", "y" ]
						},
						"modtime": "` + addToClockOrigin(3*time.Second) + `"
					}
				}
			}
		}`),
	},

	// test that describe follows camliPath:foo attributes
	{
		name: "describe-permanode-follows-camliPath",
		setup: func(im *indexMapper) index.Interface {
			pn := im.AddPermanode("perma-123")
			fakeref123 := im.AddFile("fakeref-123", 123)
			im.SetAttribute(pn, "camliPath:foo", fakeref123.String())
			return im.Index
		},
		query: "describe",
		postBody: `{
 "blobref": "perma-123",
 "rules": [
    {"attrs": ["camliPath:*"]}
 ]
}`,
		want: parseJSON(`{
  "meta": {
	` + fileResult("fakeref-123", 123) + `,
	"perma-123": {
	  "blobRef": "perma-123",
	  "camliType": "permanode",
	  "permanode": {
		"attr": {
		  "camliPath:foo": [
			"fakeref-123"
		  ]
		},
		"modtime": "` + addToClockOrigin(1*time.Second) + `"
	  }
	}
  }
}`),
	},

	// Test recent permanodes
	{
		name: "recent-1",
		setup: func(im *indexMapper) index.Interface {
			pn := im.NewPlannedPermanode("pn1")
			im.SetAttribute(pn, "title", "Some title")
			return im.Index
		},
		query: "recent",
		want: parseJSON(`{
				"recent": [
					{"blobref": "sha1-7ca7743e38854598680d94ef85348f2c48a44513",
					 "modtime": "2011-11-28T01:32:37.000123456Z",
					 "owner": "sha1-ad87ca5c78bd0ce1195c46f7c98e6025abbaf007"}
				],
				"meta": {
					  "sha1-7ca7743e38854598680d94ef85348f2c48a44513": {
		 "blobRef": "sha1-7ca7743e38854598680d94ef85348f2c48a44513",
		 "camliType": "permanode",
				 "permanode": {
				   "attr": { "title": [ "Some title" ] },
					"modtime": "` + addToClockOrigin(1*time.Second) + `"
				 },
				 "size": 534
					 }
				 }
			   }`),
	},

	// Test recent permanode of a file
	{
		name: "recent-file",
		setup: func(id *indexMapper) index.Interface {
			// Upload a basic image
			camliRootPath, err := osutil.GoPackagePath("camlistore.org")
			if err != nil {
				panic("Package camlistore.org no found in $GOPATH or $GOPATH not defined")
			}
			uploadFile := func(file string, modTime time.Time) blob.Ref {
				fileName := filepath.Join(camliRootPath, "pkg", "index", "indextest", "testdata", file)
				contents, err := ioutil.ReadFile(fileName)
				if err != nil {
					panic(err)
				}
				br, _ := id.UploadFile(file, string(contents), modTime)
				return br
			}
			dudeFileRef := uploadFile("dude.jpg", time.Time{})

			pn := id.NewPlannedPermanode("pn1")
			id.SetAttribute(pn, "camliContent", dudeFileRef.String())
			return id.Index
		},
		query: "recent",
		want: parseJSON(`{
				"recent": [
					{"blobref": "sha1-7ca7743e38854598680d94ef85348f2c48a44513",
					 "modtime": "2011-11-28T01:32:37.000123456Z",
					 "owner": "sha1-ad87ca5c78bd0ce1195c46f7c98e6025abbaf007"}
				],
				"meta": {
					  "sha1-7ca7743e38854598680d94ef85348f2c48a44513": {
		 "blobRef": "sha1-7ca7743e38854598680d94ef85348f2c48a44513",
		 "camliType": "permanode",
				 "permanode": {
				"attr": {
				  "camliContent": [
					"sha1-e3f0ee86622dda4d7e8a4a4af51117fb79dbdbbb"
				  ]
				},
				"modtime": "` + addToClockOrigin(1*time.Second) + `"
			  },
				 "size": 534
					 },
			"sha1-e3f0ee86622dda4d7e8a4a4af51117fb79dbdbbb": {
			  "blobRef": "sha1-e3f0ee86622dda4d7e8a4a4af51117fb79dbdbbb",
			  "camliType": "file",
			  "size": 184,
			  "file": {
				"fileName": "dude.jpg",
				"size": 1932,
				"mimeType": "image/jpeg",
				"wholeRef": "sha1-142b504945338158e0149d4ed25a41a522a28e88"
			  },
			  "image": {
				"width": 50,
				"height": 100
			  }
			}
				 }
			   }`),
	},

	// Test recent permanode of a file, in a collection
	{
		name: "recent-file-collec",
		setup: func(id *indexMapper) index.Interface {
			SetTestHookBug121(func() {
				time.Sleep(2 * time.Second)
			})

			// Upload a basic image
			camliRootPath, err := osutil.GoPackagePath("camlistore.org")
			if err != nil {
				panic("Package camlistore.org no found in $GOPATH or $GOPATH not defined")
			}
			uploadFile := func(file string, modTime time.Time) blob.Ref {
				fileName := filepath.Join(camliRootPath, "pkg", "index", "indextest", "testdata", file)
				contents, err := ioutil.ReadFile(fileName)
				if err != nil {
					panic(err)
				}
				br, _ := id.UploadFile(file, string(contents), modTime)
				return br
			}
			dudeFileRef := uploadFile("dude.jpg", time.Time{})
			pn := id.NewPlannedPermanode("pn1")
			id.SetAttribute(pn, "camliContent", dudeFileRef.String())
			collec := id.NewPlannedPermanode("pn2")
			id.SetAttribute(collec, "camliMember", pn.String())
			return id.Index
		},
		query: "recent",
		want: parseJSON(`{
		  "recent": [
			{
			  "blobref": "sha1-3c8b5d36bd4182c6fe802984832f197786662ccf",
			  "modtime": "2011-11-28T01:32:38.000123456Z",
			  "owner": "sha1-ad87ca5c78bd0ce1195c46f7c98e6025abbaf007"
			},
			{
			  "blobref": "sha1-7ca7743e38854598680d94ef85348f2c48a44513",
			  "modtime": "2011-11-28T01:32:37.000123456Z",
			  "owner": "sha1-ad87ca5c78bd0ce1195c46f7c98e6025abbaf007"
			}
		  ],
		  "meta": {
			"sha1-3c8b5d36bd4182c6fe802984832f197786662ccf": {
			  "blobRef": "sha1-3c8b5d36bd4182c6fe802984832f197786662ccf",
			  "camliType": "permanode",
			  "size": 534,
			  "permanode": {
				"attr": {
				  "camliMember": [
					"sha1-7ca7743e38854598680d94ef85348f2c48a44513"
				  ]
				},
				"modtime": "` + addToClockOrigin(2*time.Second) + `"
			  }
			},
			"sha1-7ca7743e38854598680d94ef85348f2c48a44513": {
			  "blobRef": "sha1-7ca7743e38854598680d94ef85348f2c48a44513",
			  "camliType": "permanode",
			  "size": 534,
			  "permanode": {
				"attr": {
				  "camliContent": [
					"sha1-e3f0ee86622dda4d7e8a4a4af51117fb79dbdbbb"
				  ]
				},
				"modtime": "` + addToClockOrigin(1*time.Second) + `"
			  }
			},
			"sha1-e3f0ee86622dda4d7e8a4a4af51117fb79dbdbbb": {
			  "blobRef": "sha1-e3f0ee86622dda4d7e8a4a4af51117fb79dbdbbb",
			  "camliType": "file",
			  "size": 184,
			  "file": {
				"fileName": "dude.jpg",
				"size": 1932,
				"mimeType": "image/jpeg",
				"wholeRef": "sha1-142b504945338158e0149d4ed25a41a522a28e88"
			  },
			  "image": {
				"width": 50,
				"height": 100
			  }
			}
		  }
		}`),
	},

	// Test recent permanodes with thumbnails
	{
		name: "recent-thumbs",
		setup: func(id *indexMapper) index.Interface {
			pn := id.NewPlannedPermanode("pn1")
			id.SetAttribute(pn, "title", "Some title")
			return id.Index
		},
		query: "recent?thumbnails=100",
		want: parseJSON(`{
				"recent": [
					{"blobref": "sha1-7ca7743e38854598680d94ef85348f2c48a44513",
					 "modtime": "2011-11-28T01:32:37.000123456Z",
					 "owner": "sha1-ad87ca5c78bd0ce1195c46f7c98e6025abbaf007"}
				],
				"meta": {
				   "sha1-7ca7743e38854598680d94ef85348f2c48a44513": {
		 "blobRef": "sha1-7ca7743e38854598680d94ef85348f2c48a44513",
		 "camliType": "permanode",
				 "permanode": {
				   "attr": { "title": [ "Some title" ] },
					"modtime": "` + addToClockOrigin(1*time.Second) + `"
				 },
				 "size": 534
					}
				}
			   }`),
	},

	// edgeto handler: put a permanode (member) in two parent
	// permanodes, then delete the second and verify that edges
	// back from member only reveal the first parent.
	{
		name: "edge-to",
		setup: func(id *indexMapper) index.Interface {
			parent1 := id.NewPlannedPermanode("pn1") // sha1-7ca7743e38854598680d94ef85348f2c48a44513
			parent2 := id.NewPlannedPermanode("pn2")
			member := id.NewPlannedPermanode("member") // always sha1-9ca84f904a9bc59e6599a53f0a3927636a6dbcae
			id.AddAttribute(parent1, "camliMember", member.String())
			id.AddAttribute(parent2, "camliMember", member.String())
			id.DelAttribute(parent2, "camliMember", "")
			return id.Index
		},
		query: "edgesto?blobref=sha1-9ca84f904a9bc59e6599a53f0a3927636a6dbcae",
		want: parseJSON(`{
			"toRef": "sha1-9ca84f904a9bc59e6599a53f0a3927636a6dbcae",
			"edgesTo": [
				{"from": "sha1-7ca7743e38854598680d94ef85348f2c48a44513",
				"fromType": "permanode"}
				]
			}`),
	},
}

func permResult(key string, title string, d time.Duration) string {
	return `"` + key + `": ` + marshalJSON(DescribedBlob{
		BlobRef:   blob.MustParse(key),
		CamliType: "permanode",
		Permanode: &DescribedPermanode{
			Attr:    url.Values{"title": []string{title}},
			ModTime: test.ClockOrigin.Add(d).UTC(),
		},
	})
}

func fileResult(key string, size int64) string {
	return `"` + key + `": ` + marshalJSON(DescribedBlob{
		BlobRef:   blob.MustParse(key),
		CamliType: "file",
		File: &camtypes.FileInfo{
			FileName: strings.Replace(key, "-", "", 1),
			Size:     size,
			WholeRef: blob.MustParse("whole" + key),
		},
	})
}

func marshalJSON(v interface{}) string {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(b)
}

func jmap(v interface{}) map[string]interface{} {
	m := make(map[string]interface{})
	if err := json.NewDecoder(strings.NewReader(marshalJSON(v))).Decode(&m); err != nil {
		panic(err)
	}
	return m
}

func checkNoDups(sliceName string, tests []handlerTest) {
	seen := map[string]bool{}
	for _, tt := range tests {
		if seen[tt.name] {
			panic(fmt.Sprintf("duplicate handlerTest named %q in var %s", tt.name, sliceName))
		}
		seen[tt.name] = true
	}
}

func init() {
	checkNoDups("handlerTests", handlerTests)
}

func (ht handlerTest) test(t *testing.T) {
	SetTestHookBug121(func() {})

	im := indexMapper{
		IndexDeps: indextest.NewIndexDeps(index.NewMemoryIndex()),
		Refs:      make(map[string]blob.Ref),
	}
	idx := ht.setup(&im)

	h := NewHandler(idx, im.IndexDeps.SignerBlobRef)

	var body io.Reader
	var method = "GET"
	if ht.postBody != "" {
		method = "POST"
		var pm map[string]interface{}
		err := json.Unmarshal([]byte(ht.postBody), &pm)
		if err != nil {
			t.Fatalf("%s: bad post body: %v", ht.name, err)
		}
		p, err := json.Marshal(im.MapMap(pm))
		if err != nil {
			t.Fatalf("%s: bad post body: %v", ht.name, err)
		}
		body = bytes.NewReader(p)
	}
	req, err := http.NewRequest(method, im.MapUrl("/camli/search/"+ht.query), body)
	if err != nil {
		t.Fatalf("%s: bad query: %v", ht.name, err)
	}
	req.Header.Set(httputil.PathSuffixHeader, req.URL.Path[1:])

	rr := httptest.NewRecorder()
	rr.Body = new(bytes.Buffer)

	h.ServeHTTP(rr, req)
	got := rr.Body.Bytes()

	if len(ht.wantDescribed) > 0 {
		dr := new(DescribeResponse)
		if err := json.NewDecoder(bytes.NewReader(got)).Decode(dr); err != nil {
			t.Fatalf("On test %s: Non-JSON response: %s", ht.name, got)
		}
		var gotDesc []string
		for k := range dr.Meta {
			gotDesc = append(gotDesc, k)
		}
		wantDescribed := make([]string, len(ht.wantDescribed))
		for i, key := range ht.wantDescribed {
			wantDescribed[i] = im.Map(key)
		}
		sort.Strings(wantDescribed)
		sort.Strings(gotDesc)
		if !reflect.DeepEqual(gotDesc, wantDescribed) {
			t.Errorf("On test %s: described blobs:\n%v\nwant:\n%v\n",
				ht.name, gotDesc, wantDescribed)
		}
		if ht.want == nil {
			return
		}
	}

	mapped := im.MapMap(ht.want)
	want, _ := json.MarshalIndent(mapped, "", "  ")
	trim := bytes.TrimSpace

	if bytes.Equal(trim(got), trim(want)) {
		return
	}

	// Try with re-encoded got, since the JSON ordering
	// and sizes we don't care about doesn't matter.
	gotj := parseJSON(string(got))
	cleanZeroMetaSizes(gotj, mapped)
	got2, _ := json.MarshalIndent(gotj, "", "  ")
	if bytes.Equal(got2, want) {
		return
	}
	diff := test.Diff(want, got2)

	t.Errorf("test %s:\nwant: %s\n got: %s\ndiff:\n%s", ht.name, want, got, diff)
}

// cleanZeroMetaSizes sets the size for permanodes in res
// from base if the size is missing or zero.
func cleanZeroMetaSizes(res, base map[string]interface{}) {
	i := res["meta"]
	rmeta, ok := i.(map[string]interface{})
	if !ok {
		return
	}
	i = base["meta"]
	bmeta, ok := i.(map[string]interface{})
	if !ok {
		return
	}
	for k, v := range bmeta {
		i = rmeta[k]
		rmap, ok := i.(map[string]interface{})
		if !ok {
			continue
		}
		bmap, ok := v.(map[string]interface{})
		if !ok {
			continue
		}
		i = bmap["size"]
		bsize, ok := i.(float64)
		switch {
		case !ok:
			delete(rmap, "size")
		case bsize == 0:
			rmap["size"] = i
		}
	}
}

func TestHandler(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
		return
	}
	defer SetTestHookBug121(func() {})
	for _, ht := range handlerTests {
		ht.test(t)
	}
}
