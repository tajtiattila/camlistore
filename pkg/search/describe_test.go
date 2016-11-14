/*
Copyright 2014 The Camlistore Authors

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
	"encoding/binary"
	"fmt"
	"image"
	"image/jpeg"
	"testing"
	"time"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/index"
	"camlistore.org/pkg/index/indextest"
	"camlistore.org/pkg/search"

	"golang.org/x/net/context"
)

func searchDescribeSetup(im *indexMapper) index.Interface {

	addPermanode := func(key string, attrs ...string) string {
		return im.AddPermanode(key, attrs...).String()
	}

	addFileWithLocation := func(key string, lat, long float64) string {
		fileRef, _ := im.UploadFile(key+".jpg",
			exifFileContentLatLong(lat, long), time.Time{})
		im.Refs[key] = fileRef
		return fileRef.String()
	}

	abc123cc := addPermanode("abc-123cc",
		"name", "leaf",
	)
	abc123c1 := addPermanode("abc-123c1",
		"some", "image",
	)
	abc123c := addPermanode("abc-123c",
		"camliContent", abc123cc,
		"camliImageContent", abc123c1,
	)
	abc8881 := addPermanode("abc-8881",
		"name", "leaf8881",
	)
	abc888 := addPermanode("abc-888",
		"camliContent", abc8881,
	)
	addPermanode("abc-123",
		"camliContent", abc123c,
		"camliImageContent", abc888,
	)

	somevenuepic0 := addPermanode("somevenuepic-0",
		"foo", "bar",
	)
	venuepic1 := addPermanode("venuepic-1",
		"camliContent", somevenuepic0,
	)
	venuepicset123 := addPermanode("venuepicset-123",
		"camliPath:1.jpg", venuepic1,
	)
	fourvenue123 := addPermanode("fourvenue-123",
		"camliNodeType", "foursquare.com:venue",
		"camliPath:photos", venuepicset123,
		"latitude", "12",
		"longitude", "34",
	)
	addPermanode("fourcheckin-0",
		"camliNodeType", "foursquare.com:checkin",
		"foursquareVenuePermanode", fourvenue123,
	)
	somevenuepic2 := addPermanode("somevenuepic-2",
		"foo", "baz",
	)
	venuepic2 := addPermanode("venuepic-2",
		"camliContent", somevenuepic2,
	)

	homedir2 := addPermanode("homedir-2",
		"foo", "bar",
	)
	homedir1 := addPermanode("homedir-1",
		"camliPath:subdir.2", homedir2,
	)
	addPermanode("homedir-0",
		"camliPath:subdir.1", homedir1,
	)

	addPermanode("set-0",
		"camliMember", venuepic1,
		"camliMember", venuepic2,
	)

	filewithloc0 := addFileWithLocation("filewithloc-0", 45, 56)
	addPermanode("location-0",
		"camliContent", filewithloc0,
	)

	addPermanode("locationpriority-1",
		"latitude", "67",
		"longitude", "78",
		"camliNodeType", "foursquare.com:checkin",
		"foursquareVenuePermanode", fourvenue123,
		"camliContent", filewithloc0,
	)

	addPermanode("locationpriority-2",
		"camliNodeType", "foursquare.com:checkin",
		"foursquareVenuePermanode", fourvenue123,
		"camliContent", filewithloc0,
	)

	addPermanode("locationoverride-1",
		"latitude", "67",
		"longitude", "78",
		"camliContent", filewithloc0,
	)

	addPermanode("locationoverride-2",
		"latitude", "67",
		"longitude", "78",
		"camliNodeType", "foursquare.com:checkin",
		"foursquareVenuePermanode", fourvenue123,
	)

	return im.Index
}

var searchDescribeTests = []handlerTest{
	{
		name:     "null",
		postBody: marshalJSON(&search.DescribeRequest{}),
		want: jmap(&search.DescribeResponse{
			Meta: search.MetaMap{},
		}),
	},

	{
		name: "single",
		postBody: marshalJSON(&search.DescribeRequest{
			BlobRef: blob.MustParse("abc-123"),
		}),
		wantDescribed: []string{"abc-123"},
	},

	{
		name: "follow all camliContent",
		postBody: marshalJSON(&search.DescribeRequest{
			BlobRef: blob.MustParse("abc-123"),
			Rules: []*search.DescribeRule{
				{
					Attrs: []string{"camliContent"},
				},
			},
		}),
		wantDescribed: []string{"abc-123", "abc-123c", "abc-123cc"},
	},

	{
		name: "follow only root camliContent",
		postBody: marshalJSON(&search.DescribeRequest{
			BlobRef: blob.MustParse("abc-123"),
			Rules: []*search.DescribeRule{
				{
					IfResultRoot: true,
					Attrs:        []string{"camliContent"},
				},
			},
		}),
		wantDescribed: []string{"abc-123", "abc-123c"},
	},

	{
		name: "follow all root, substring",
		postBody: marshalJSON(&search.DescribeRequest{
			BlobRef: blob.MustParse("abc-123"),
			Rules: []*search.DescribeRule{
				{
					IfResultRoot: true,
					Attrs:        []string{"camli*"},
				},
			},
		}),
		wantDescribed: []string{"abc-123", "abc-123c", "abc-888"},
	},

	{
		name: "two rules, two attrs",
		postBody: marshalJSON(&search.DescribeRequest{
			BlobRef: blob.MustParse("abc-123"),
			Rules: []*search.DescribeRule{
				{
					IfResultRoot: true,
					Attrs:        []string{"camliContent", "camliImageContent"},
				},
				{
					Attrs: []string{"camliContent"},
				},
			},
		}),
		wantDescribed: []string{"abc-123", "abc-123c", "abc-123cc", "abc-888", "abc-8881"},
	},

	{
		name: "foursquare venue photos, but not recursive camliPath explosion",
		postBody: marshalJSON(&search.DescribeRequest{
			BlobRefs: []blob.Ref{
				blob.MustParse("homedir-0"),
				blob.MustParse("fourcheckin-0"),
			},
			Rules: []*search.DescribeRule{
				{
					Attrs: []string{"camliContent", "camliContentImage"},
				},
				{
					IfCamliNodeType: "foursquare.com:checkin",
					Attrs:           []string{"foursquareVenuePermanode"},
				},
				{
					IfCamliNodeType: "foursquare.com:venue",
					Attrs:           []string{"camliPath:photos"},
					Rules: []*search.DescribeRule{
						{
							Attrs: []string{"camliPath:*"},
						},
					},
				},
			},
		}),
		wantDescribed: []string{"homedir-0", "fourcheckin-0", "fourvenue-123", "venuepicset-123", "venuepic-1", "somevenuepic-0"},
	},

	{
		name: "home dirs forever",
		postBody: marshalJSON(&search.DescribeRequest{
			BlobRefs: []blob.Ref{
				blob.MustParse("homedir-0"),
			},
			Rules: []*search.DescribeRule{
				{
					Attrs: []string{"camliPath:*"},
				},
			},
		}),
		wantDescribed: []string{"homedir-0", "homedir-1", "homedir-2"},
	},

	{
		name: "find members",
		postBody: marshalJSON(&search.DescribeRequest{
			BlobRef: blob.MustParse("set-0"),
			Rules: []*search.DescribeRule{
				{
					IfResultRoot: true,
					Attrs:        []string{"camliMember"},
					Rules: []*search.DescribeRule{
						{Attrs: []string{"camliContent"}},
					},
				},
			},
		}),
		wantDescribed: []string{"set-0", "venuepic-1", "venuepic-2", "somevenuepic-0", "somevenuepic-2"},
	},
}

func init() {
	checkNoDups("searchDescribeTests", searchDescribeTests)
}

func TestSearchDescribe(t *testing.T) {
	for _, ht := range searchDescribeTests {
		if ht.setup == nil {
			ht.setup = searchDescribeSetup
		}
		if ht.query == "" {
			ht.query = "describe"
		}
		ht.test(t)
	}
}

// should be run with -race
func TestDescribeRace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	idx := index.NewMemoryIndex()
	idxd := indextest.NewIndexDeps(idx)
	idxd.Fataler = t
	corpus, err := idxd.Index.KeepInMemory()
	if err != nil {
		t.Fatalf("error slurping index to memory: %v", err)
	}
	h := search.NewHandler(idx, idxd.SignerBlobRef)
	h.SetCorpus(corpus)
	donec := make(chan struct{})
	headstart := 500
	blobrefs := make([]blob.Ref, headstart)
	headstartc := make(chan struct{})
	go func() {
		for i := 0; i < headstart*2; i++ {
			nth := fmt.Sprintf("%d", i)
			// No need to lock the index here. It is already done within NewPlannedPermanode,
			// because it calls idxd.Index.ReceiveBlob.
			pn := idxd.NewPlannedPermanode(nth)
			idxd.SetAttribute(pn, "tag", nth)
			if i > headstart {
				continue
			}
			if i == headstart {
				headstartc <- struct{}{}
				continue
			}
			blobrefs[i] = pn
		}
	}()
	<-headstartc
	ctx := context.Background()
	go func() {
		for i := 0; i < headstart; i++ {
			br := blobrefs[i]
			res, err := h.Describe(ctx, &search.DescribeRequest{
				BlobRef: br,
				Depth:   1,
			})
			if err != nil {
				t.Fatal(err)
			}
			_, ok := res.Meta[br.String()]
			if !ok {
				t.Errorf("permanode %v wasn't in Describe response", br)
			}
		}
		donec <- struct{}{}
	}()
	<-donec
}

func TestDescribeLocation(t *testing.T) {
	tests := []struct {
		ref       string
		lat, long float64
	}{
		{"filewithloc-0", 45, 56},
		{"location-0", 45, 56},
		{"locationpriority-1", 67, 78},
		{"locationpriority-2", 12, 34},
		{"locationoverride-1", 67, 78},
		{"locationoverride-2", 67, 78},
	}

	im := indexMapper{
		IndexDeps: indextest.NewIndexDeps(index.NewMemoryIndex()),
		Refs:      make(map[string]blob.Ref),
	}
	ix := searchDescribeSetup(&im)
	ctx := context.Background()
	h := search.NewHandler(ix, im.SignerBlobRef)

	ix.RLock()
	defer ix.RUnlock()

	for _, tt := range tests {
		var err error
		br := im.Ref(tt.ref)
		res, err := h.Describe(ctx, &search.DescribeRequest{
			BlobRef: br,
			Depth:   1,
		})
		if err != nil {
			t.Errorf("Describe for %v failed: %v", br, err)
			continue
		}
		db := res.Meta[br.String()]
		if db == nil {
			t.Errorf("Describe result for %v is missing", br)
			continue
		}
		loc := db.Location
		if loc == nil {
			t.Errorf("no location in result for %v", br)
			continue
		}
		if loc.Latitude != tt.lat || loc.Longitude != tt.long {
			t.Errorf("location for %v invalid, got %f,%f want %f,%f",
				tt.ref, loc.Latitude, loc.Longitude, tt.lat, tt.long)
		}
	}
}

// exifFileContentLatLong returns the contents of a
// jpeg/exif file with GPS coordinates.
func exifFileContentLatLong(lat, long float64) string {
	var buf bytes.Buffer
	jpeg.Encode(&buf, image.NewRGBA(image.Rect(0, 0, 128, 128)), nil)
	j := buf.Bytes()

	x := rawExifLatLong(lat, long)

	app1sec := []byte{0xff, 0xe1, 0, 0}
	binary.BigEndian.PutUint16(app1sec[2:], uint16(len(x)+2))

	p := make([]byte, 0, len(j)+len(app1sec)+len(x))
	p = append(p, j[:2]...)   // ff d8
	p = append(p, app1sec...) // exif section header
	p = append(p, x...)       // raw exif
	p = append(p, j[2:]...)   // jpeg image

	return string(p)
}

// rawExifLatLong creates raw exif for lat/long
// for storage in a jpeg file.
func rawExifLatLong(lat, long float64) []byte {

	x := exifBuf{
		bo: binary.BigEndian,
		p:  []byte("MM"),
	}

	x.putUint16(42) // magic

	ifd0ofs := x.reservePtr() // room for ifd0 offset
	x.storePtr(ifd0ofs)

	const (
		gpsSubIfdTag = 0x8825

		gpsLatitudeRef  = 1
		gpsLatitude     = 2
		gpsLongitudeRef = 3
		gpsLongitude    = 4

		typeAscii    = 2
		typeLong     = 4
		typeRational = 5
	)

	// IFD0
	x.storePtr(ifd0ofs)
	x.putUint16(1) // 1 tag

	x.putTag(gpsSubIfdTag, typeLong, 1)
	gpsofs := x.reservePtr()

	// IFD1
	x.putUint32(0) // no IFD1

	// GPS sub-IFD
	x.storePtr(gpsofs)
	x.putUint16(4) // 4 tags

	x.putTag(gpsLatitudeRef, typeAscii, 2)
	if lat >= 0 {
		x.next(4)[0] = 'N'
	} else {
		x.next(4)[0] = 'S'
	}

	x.putTag(gpsLatitude, typeRational, 3)
	latptr := x.reservePtr()

	x.putTag(gpsLongitudeRef, typeAscii, 2)
	if long >= 0 {
		x.next(4)[0] = 'E'
	} else {
		x.next(4)[0] = 'W'
	}

	x.putTag(gpsLongitude, typeRational, 3)
	longptr := x.reservePtr()

	// write data referenced in GPS sub-IFD
	x.storePtr(latptr)
	x.putDegMinSecRat(lat)

	x.storePtr(longptr)
	x.putDegMinSecRat(long)

	return append([]byte("Exif\x00\x00"), x.p...)
}

type exifBuf struct {
	bo binary.ByteOrder
	p  []byte
}

func (x *exifBuf) next(n int) []byte {
	l := len(x.p)
	x.p = append(x.p, make([]byte, n)...)
	return x.p[l:]
}

func (x *exifBuf) putTag(tag, typ uint16, len uint32) {
	x.putUint16(tag)
	x.putUint16(typ)
	x.putUint32(len)
}

func (x *exifBuf) putUint16(n uint16) { x.bo.PutUint16(x.next(2), n) }
func (x *exifBuf) putUint32(n uint32) { x.bo.PutUint32(x.next(4), n) }

func (x *exifBuf) putDegMinSecRat(v float64) {
	if v < 0 {
		v = -v
	}
	deg := uint32(v)
	v = 60 * (v - float64(deg))
	min := uint32(v)
	v = 60 * (v - float64(min))
	μsec := uint32(v * 1e6)

	x.putUint32(deg)
	x.putUint32(1)
	x.putUint32(min)
	x.putUint32(1)
	x.putUint32(μsec)
	x.putUint32(1e6)
}

func (x *exifBuf) reservePtr() int {
	l := len(x.p)
	x.next(4)
	return l
}

func (x *exifBuf) storePtr(ofs int) { x.bo.PutUint32(x.p[ofs:], uint32(len(x.p))) }
